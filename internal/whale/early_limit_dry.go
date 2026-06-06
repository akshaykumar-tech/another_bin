package whale

import (
	"context"
	"log"
	"time"
)

type reverseLimitPending struct {
	Symbol     string
	TradeSide  Side
	LimitPx    float64
	SignalAt   time.Time
	EntryRefAt time.Time
	ExpiresAt  time.Time
	Margin     float64
	Leverage   int
	Sig        Signal
}

type earlyDryPath int

const (
	earlyDrySame earlyDryPath = iota
	earlyDryReverse
)

func earlyLimitFillAtPrice(price, limitPx float64, short bool) bool {
	if price <= 0 || limitPx <= 0 {
		return false
	}
	if short {
		return price >= limitPx
	}
	return price <= limitPx
}

func (e *Executor) initEarlyDryMaps() {
	if e.revPending == nil {
		e.revPending = make(map[string]*reverseLimitPending)
	}
	if e.samePending == nil {
		e.samePending = make(map[string]*reverseLimitPending)
	}
	if e.lastDrySameTrade == nil {
		e.lastDrySameTrade = make(map[string]time.Time)
	}
	if e.dryRevOpen == nil {
		e.dryRevOpen = make(map[string]*simPosition)
	}
	if e.revClosing == nil {
		e.revClosing = make(map[string]bool)
	}
	if e.lastRevTrade == nil {
		e.lastRevTrade = make(map[string]time.Time)
	}
}

func (e *Executor) LastRevTradeTime(sym string) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initEarlyDryMaps()
	return e.lastRevTrade[sym]
}

func (e *Executor) revCooldownOK(sym string, at time.Time) bool {
	e.initEarlyDryMaps()
	if t, ok := e.lastRevTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		return false
	}
	return true
}

func (e *Executor) countDrySameSlotsLocked() int {
	seen := make(map[string]struct{})
	for s := range e.dryOpen {
		seen[s] = struct{}{}
	}
	for s := range e.samePending {
		seen[s] = struct{}{}
	}
	return len(seen)
}

func (e *Executor) CanEarlySameDry(sym string, at time.Time) bool {
	if !e.cfg.DryRun {
		return false
	}
	if !e.cfg.Early.DrySameEnabled && !e.cfg.EarlyVol3SameMode() {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initEarlyDryMaps()
	if e.countDrySameSlotsLocked() >= e.cfg.MaxOpenPositions {
		return false
	}
	if t, ok := e.lastDrySameTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		return false
	}
	if _, busy := e.dryOpen[sym]; busy {
		return false
	}
	if _, pend := e.samePending[sym]; pend {
		return false
	}
	return true
}

func (e *Executor) CanEarlyLive(sym string, at time.Time) bool {
	if !e.cfg.EarlyLiveTrade || !e.client.Configured() {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initSameLimitLiveMaps()
	if e.countEarlyLiveSlotsLocked() >= e.cfg.EarlyMaxOpenLivePositions() {
		return false
	}
	if t, ok := e.lastSameLimitLiveTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		return false
	}
	if _, busy := e.active[sym]; busy {
		return false
	}
	if e.earlyLive != nil {
		if _, busy := e.earlyLive[sym]; busy {
			return false
		}
	}
	if e.sameLimitLivePending != nil {
		if _, pend := e.sameLimitLivePending[sym]; pend {
			return false
		}
	}
	return true
}

func (e *Executor) HasDryRevPosition(sym string) bool {
	_, ok := e.dryRevSyms.Load(sym)
	return ok
}

func (e *Executor) trackDryRevOpen(sym string) { e.dryRevSyms.Store(sym, struct{}{}) }
func (e *Executor) untrackDryRevOpen(sym string) { e.dryRevSyms.Delete(sym) }

func (e *Executor) marginForEarlyPath(sig *Signal, path earlyDryPath) (margin float64, leverage int) {
	c := e.cfg
	switch path {
	case earlyDryReverse:
		if c.EarlyReverseMarginUSDT > 0 {
			margin = c.EarlyReverseMarginUSDT
		} else if c.MarginUSDT > 0 {
			margin = c.MarginUSDT
		}
		lev := c.EarlyReverseLeverage
		if lev <= 0 {
			lev = c.Leverage
		}
		if lev <= 0 {
			lev = 1
		}
		if margin > 0 {
			return margin, lev
		}
		alloc := c.EarlyReverseAllocationPercent
		if alloc <= 0 {
			alloc = c.AllocationPercent
		}
		return e.marginFromAllocation(sig, alloc), lev
	default:
		if c.EarlySameMarginUSDT > 0 {
			margin = c.EarlySameMarginUSDT
		} else if c.MarginUSDT > 0 {
			margin = c.MarginUSDT
		}
		lev := c.EarlySameLeverage
		if lev <= 0 {
			lev = c.Leverage
		}
		if lev <= 0 {
			lev = 1
		}
		if margin > 0 {
			return margin, lev
		}
		alloc := c.EarlySameAllocationPercent
		if alloc <= 0 {
			alloc = c.AllocationPercent
		}
		return e.marginFromAllocation(sig, alloc), lev
	}
}

func (e *Executor) marginFromAllocation(sig *Signal, pct float64) float64 {
	capital := e.cfg.CapitalUSDT
	if pct <= 0 {
		if sig.Mega {
			pct = e.cfg.Risk.MegaRiskPercent
		} else {
			pct = e.cfg.Risk.NormalRiskPercent
		}
	} else {
		pct = pct / 100
	}
	maxPct := e.cfg.Risk.MaxPositionPercent / 100
	if pct > maxPct {
		pct = maxPct
	}
	return capital * pct
}

func (e *Executor) earlyScheduledExitAt(signalAt time.Time) time.Time {
	if signalAt.IsZero() {
		return time.Time{}
	}
	return signalAt.Add(e.dryHoldDuration(SignalEarly))
}

func (p *simPosition) exitDeadline(hold time.Duration) time.Time {
	if p != nil && !p.ScheduledExitAt.IsZero() {
		return p.ScheduledExitAt
	}
	return p.OpenedAt.Add(hold)
}

func (e *Executor) limitFillWindow() time.Duration {
	sec := e.cfg.Early.LiveLimitFillSec
	if sec <= 0 {
		sec = 1800
	}
	return time.Duration(sec * float64(time.Second))
}

func (e *Executor) TryStartSameLimit(ctx context.Context, sig *Signal, volAccel float64) {
	if sig == nil || !e.cfg.DryRun {
		return
	}
	if !e.cfg.Early.DrySameEnabled && !e.cfg.EarlyVol3SameMode() {
		return
	}
	minVol := e.cfg.Early.MinVolAccel
	if minVol > 0 && volAccel < minVol {
		return
	}
	sym := sig.Symbol
	if e.cfg.IsSymbolBlocked(sym) {
		return
	}

	e.mu.Lock()
	e.initEarlyDryMaps()
	if e.countDrySameSlotsLocked() >= e.cfg.MaxOpenPositions {
		e.mu.Unlock()
		return
	}
	if _, busy := e.dryOpen[sym]; busy {
		e.mu.Unlock()
		return
	}
	if _, pend := e.samePending[sym]; pend {
		e.mu.Unlock()
		return
	}
	at := sig.RecvAt
	if at.IsZero() {
		at = time.Now()
	}
	if t, ok := e.lastDrySameTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	margin, lev := e.marginForEarlyPath(sig, earlyDrySame)
	tradeSide := e.cfg.TradeSide(sig.Side)
	limitPx := EarlySameDirLimitPxFromSignal(e.cfg, sig)
	if limitPx <= 0 {
		return
	}
	entryRefAt := at.Add(EarlyEntryDelay(e.cfg))
	window := e.limitFillWindow()
	expires := entryRefAt.Add(window)

	pend := &reverseLimitPending{
		Symbol: sym, TradeSide: tradeSide, LimitPx: limitPx,
		SignalAt: at, EntryRefAt: entryRefAt, ExpiresAt: expires, Margin: margin, Leverage: lev,
		Sig: *sig,
	}
	e.mu.Lock()
	e.samePending[sym] = pend
	e.lastDrySameTrade[sym] = at
	e.mu.Unlock()

	log.Printf("[early] SAME_LIMIT pending %s %s limit=%.6f vol=%.1fx window=%s",
		tradeSide, sym, limitPx, volAccel, window.Round(time.Second))
}

func (e *Executor) OnSameLimitTick(ctx context.Context, sym string, price float64, at time.Time) {
	if !e.cfg.DryRun || price <= 0 {
		return
	}
	if !e.cfg.Early.DrySameEnabled && !e.cfg.EarlyVol3SameMode() {
		return
	}
	e.mu.Lock()
	e.initEarlyDryMaps()
	pend, ok := e.samePending[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	if !pend.EntryRefAt.IsZero() && at.Before(pend.EntryRefAt) {
		e.mu.Unlock()
		return
	}
	if !at.Before(pend.ExpiresAt) {
		delete(e.samePending, sym)
		e.mu.Unlock()
		log.Printf("[early] SAME_LIMIT expired %s (no fill)", sym)
		return
	}
	if !earlyLimitFillAtPrice(price, pend.LimitPx, pend.TradeSide == SideSell) {
		e.mu.Unlock()
		return
	}
	delete(e.samePending, sym)
	sig := pend.Sig
	tradeSide := pend.TradeSide
	entry := price
	margin := pend.Margin
	lev := pend.Leverage
	e.mu.Unlock()

	absMove := sig.MovePct
	if absMove < 0 {
		absMove = -absMove
	}
	scheduledExit := e.earlyScheduledExitAt(pend.SignalAt)
	pos := &simPosition{
		Symbol: sym, Side: tradeSide, EntryPrice: entry, MarginUSDT: margin, Leverage: lev,
		OpenedAt: at, PeakPrice: entry, LastPrice: entry, SignalAbsMovePct: absMove,
		SignalKind: SignalEarly, ScheduledExitAt: scheduledExit,
	}
	e.mu.Lock()
	e.dryOpen[sym] = pos
	e.trackDryOpen(sym)
	e.openCount++
	e.mu.Unlock()

	log.Printf("[early] SAME_LIMIT fill %s %s @ %.6f margin=%.2f lev=%dx exit_at=%s",
		tradeSide, sym, entry, margin, lev, scheduledExit.Format("15:04:05"))
	if e.journal != nil {
		e.journal.LogEntry(&sig, tradeSide, entry, margin, lev, "same_limit")
	}
	if !at.Before(scheduledExit) {
		e.closeDry(sym, "early_hold")
		return
	}
	go e.dryRunTimeout(ctx, sym, SignalEarly)
	go e.manageDryRunMarkPoll(ctx, sym, SignalEarly)
}

func (e *Executor) TryStartReverseLimit(ctx context.Context, sig *Signal, volAccel float64) {
	if sig == nil || !e.cfg.DryRun || !e.cfg.Early.DryReverseLimitEnabled {
		return
	}
	minVol := e.cfg.Early.MinVolAccel
	if minVol > 0 && volAccel < minVol {
		return
	}
	sym := sig.Symbol
	if e.cfg.IsSymbolBlocked(sym) {
		return
	}

	e.mu.Lock()
	e.initEarlyDryMaps()
	if e.countOpenSlotsLocked() >= e.cfg.MaxOpenPositions {
		e.mu.Unlock()
		return
	}
	if _, busy := e.dryRevOpen[sym]; busy {
		e.mu.Unlock()
		return
	}
	if _, pend := e.revPending[sym]; pend {
		e.mu.Unlock()
		return
	}
	at := sig.RecvAt
	if at.IsZero() {
		at = time.Now()
	}
	if t, ok := e.lastRevTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	margin, lev := e.marginForEarlyPath(sig, earlyDryReverse)
	tradeSide := oppositeSide(sig.Side)
	entryRefAt := at.Add(EarlyEntryDelay(e.cfg))
	limitPx := EarlySameDirLimitPxFromSignal(e.cfg, sig)
	if limitPx <= 0 {
		return
	}
	window := e.limitFillWindow()
	expires := entryRefAt.Add(window)

	pend := &reverseLimitPending{
		Symbol: sym, TradeSide: tradeSide, LimitPx: limitPx,
		SignalAt: at, EntryRefAt: entryRefAt, ExpiresAt: expires, Margin: margin, Leverage: lev,
		Sig: *sig,
	}
	e.mu.Lock()
	e.revPending[sym] = pend
	e.lastRevTrade[sym] = at // one signal slot per cooldown (fill or miss)
	e.mu.Unlock()

	log.Printf("[early] REV_LIMIT pending %s %s limit=%.6f vol=%.1fx window=%s",
		tradeSide, sym, limitPx, volAccel, window.Round(time.Second))
}

func (e *Executor) OnRevLimitTick(ctx context.Context, sym string, price float64, at time.Time) {
	if !e.cfg.DryRun || !e.cfg.Early.DryReverseLimitEnabled || price <= 0 {
		return
	}
	e.mu.Lock()
	e.initEarlyDryMaps()
	pend, ok := e.revPending[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	if !pend.EntryRefAt.IsZero() && at.Before(pend.EntryRefAt) {
		e.mu.Unlock()
		return
	}
	if !at.Before(pend.ExpiresAt) {
		delete(e.revPending, sym)
		e.mu.Unlock()
		log.Printf("[early] REV_LIMIT expired %s (no fill)", sym)
		return
	}
	if !earlyLimitFillAtPrice(price, pend.LimitPx, pend.TradeSide == SideSell) {
		e.mu.Unlock()
		return
	}
	delete(e.revPending, sym)
	sig := pend.Sig
	tradeSide := pend.TradeSide
	entry := price
	margin := pend.Margin
	lev := pend.Leverage
	e.mu.Unlock()

	absMove := sig.MovePct
	if absMove < 0 {
		absMove = -absMove
	}
	scheduledExit := e.earlyScheduledExitAt(pend.SignalAt)
	pos := &simPosition{
		Symbol: sym, Side: tradeSide, EntryPrice: entry, MarginUSDT: margin, Leverage: lev,
		OpenedAt: at, PeakPrice: entry, LastPrice: entry, SignalAbsMovePct: absMove,
		SignalKind: SignalEarly, ScheduledExitAt: scheduledExit,
	}
	e.mu.Lock()
	e.dryRevOpen[sym] = pos
	e.trackDryRevOpen(sym)
	e.openCount++
	e.lastRevTrade[sym] = pend.SignalAt
	e.mu.Unlock()

	log.Printf("[early] REV_LIMIT fill %s %s @ %.6f (signal %s) margin=%.2f lev=%dx exit_at=%s",
		tradeSide, sym, entry, sig.Side, margin, lev, scheduledExit.Format("15:04:05"))
	if e.journal != nil {
		e.journal.LogEntry(&sig, tradeSide, entry, margin, lev, "rev_limit")
	}
	if !at.Before(scheduledExit) {
		e.closeDryRev(sym, "early_hold")
		return
	}
	go e.dryRevRunTimeout(ctx, sym)
	go e.manageDryRevMarkPoll(ctx, sym)
}

func (e *Executor) OnRevPriceTick(sym string, price float64, at time.Time) {
	if !e.cfg.DryRun || !e.cfg.UsesTickDrySim() || price <= 0 {
		return
	}
	e.mu.Lock()
	e.initEarlyDryMaps()
	if e.revClosing[sym] {
		e.mu.Unlock()
		return
	}
	pos, ok := e.dryRevOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	px := applySlippage(price, pos.Side, e.cfg.DryExitSlippageBps, false)
	pos.LastPrice = px
	e.mu.Unlock()
	_ = e.dryRevExitStep(pos, px, at)
}

func (e *Executor) dryRevRunTimeout(ctx context.Context, sym string) {
	hold := e.dryHoldDuration(SignalEarly)
	e.mu.Lock()
	e.initEarlyDryMaps()
	pos, ok := e.dryRevOpen[sym]
	e.mu.Unlock()
	if !ok {
		return
	}
	deadline := pos.exitDeadline(hold)
	wait := time.Until(deadline)
	if wait < 0 {
		wait = 0
	}
	select {
	case <-ctx.Done():
		e.closeDryRev(sym, "ctx")
	case <-time.After(wait):
		e.closeDryRev(sym, "early_hold")
	}
}

func (e *Executor) manageDryRevMarkPoll(ctx context.Context, sym string) {
	e.mu.Lock()
	pos, ok := e.dryRevOpen[sym]
	e.mu.Unlock()
	if !ok {
		return
	}
	deadline := pos.exitDeadline(e.dryHoldDuration(SignalEarly))
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			e.closeDryRev(sym, "ctx")
			return
		case <-time.After(time.Until(deadline)):
			e.closeDryRev(sym, "early_hold")
			return
		case <-tick.C:
			e.mu.Lock()
			if e.revClosing[sym] {
				e.mu.Unlock()
				return
			}
			pos, ok = e.dryRevOpen[sym]
			if !ok {
				e.mu.Unlock()
				return
			}
			e.mu.Unlock()

			mp, err := e.client.MarkPrice(sym)
			if err != nil || mp <= 0 {
				continue
			}
			e.mu.Lock()
			if e.revClosing[sym] {
				e.mu.Unlock()
				return
			}
			pos, ok = e.dryRevOpen[sym]
			if !ok {
				e.mu.Unlock()
				return
			}
			px := applySlippage(mp, pos.Side, e.cfg.DryExitSlippageBps, false)
			pos.LastPrice = px
			e.mu.Unlock()
			if e.dryRevExitStep(pos, px, time.Now()) {
				return
			}
		}
	}
}

func (e *Executor) dryRevExitStep(pos *simPosition, price float64, at time.Time) bool {
	if pos == nil || pos.SignalKind != SignalEarly {
		return false
	}
	hold := e.dryHoldDuration(SignalEarly)
	if at.Before(pos.exitDeadline(hold)) {
		return false
	}
	if !e.claimDryRevExit(pos.Symbol) {
		return true
	}
	e.logExit(pos, price, at, "early_hold", 0)
	if e.journal != nil {
		e.journal.LogExit(e.cfg.RiskForExit(), pos, price, at, "early_hold", 0)
	}
	e.finishDryRevClose(pos.Symbol)
	return true
}

func (e *Executor) claimDryRevExit(sym string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initEarlyDryMaps()
	if e.revClosing[sym] {
		return false
	}
	if _, ok := e.dryRevOpen[sym]; !ok {
		return false
	}
	e.revClosing[sym] = true
	return true
}

func (e *Executor) finishDryRevClose(sym string) {
	e.mu.Lock()
	delete(e.dryRevOpen, sym)
	e.untrackDryRevOpen(sym)
	delete(e.revClosing, sym)
	e.openCount--
	e.mu.Unlock()
}

func (e *Executor) closeDryRev(sym, reason string) {
	e.mu.Lock()
	e.initEarlyDryMaps()
	if e.revClosing[sym] {
		e.mu.Unlock()
		return
	}
	pos, ok := e.dryRevOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	e.revClosing[sym] = true
	delete(e.dryRevOpen, sym)
	e.untrackDryRevOpen(sym)
	e.openCount--
	e.mu.Unlock()

	exit := pos.LastPrice
	if exit <= 0 {
		exit, _ = e.client.MarkPrice(sym)
	}
	if exit <= 0 {
		exit = pos.EntryPrice
	}
	at := time.Now()
	e.logExit(pos, exit, at, reason, 0)
	if e.journal != nil {
		e.journal.LogExit(e.cfg.RiskForExit(), pos, exit, at, reason, 0)
	}
	delete(e.revClosing, sym)
}
