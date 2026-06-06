package whale

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"crypto_announcements_go/internal/binance"
)

type Executor struct {
	cfg     Config
	client  *binance.FuturesClient
	journal *TradeJournal
	focus   *FocusController

	mu         sync.Mutex
	openCount  int
	lastTrade  map[string]time.Time
	active     map[string]*position
	dryOpen    map[string]*simPosition
	dryPartial map[string]float64
	reverseLive map[string]*reversePosition
	earlyLive   map[string]*earlyLivePosition
	closing     map[string]bool // per-symbol exit in progress (avoids duplicate EXIT logs)
	dryOpenSyms sync.Map                    // fast HasDryPosition without scanning all symbols
	// Early reverse limit dry (independent of same-dir dryOpen):
	revPending   map[string]*reverseLimitPending
	dryRevOpen   map[string]*simPosition
	revClosing   map[string]bool
	lastRevTrade map[string]time.Time
	dryRevSyms   sync.Map
	// Same-direction limit dry (fills into dryOpen):
	samePending map[string]*reverseLimitPending
	lastDrySameTrade map[string]time.Time
	// Reverse limit live (Binance GTX limit → hold → market exit):
	revLimitLivePending map[string]*reverseLimitLivePending
	revLimitLiveOpen    map[string]*earlyLivePosition
	lastRevLiveTrade    map[string]time.Time
	// Same-direction limit live (GTX limit → TP or signal+hold market exit):
	sameLimitLivePending map[string]*reverseLimitLivePending
	lastSameLimitLiveTrade map[string]time.Time
}

// countOpenSlotsLocked returns unique symbols with dry, live, or reverse-live (caller must hold e.mu).
func (e *Executor) countOpenSlotsLocked() int {
	seen := make(map[string]struct{}, len(e.dryOpen)+len(e.active)+len(e.reverseLive))
	for s := range e.dryOpen {
		seen[s] = struct{}{}
	}
	for s := range e.active {
		seen[s] = struct{}{}
	}
	for s := range e.reverseLive {
		seen[s] = struct{}{}
	}
	for s := range e.earlyLive {
		seen[s] = struct{}{}
	}
	for s := range e.dryRevOpen {
		seen[s] = struct{}{}
	}
	for s := range e.revLimitLiveOpen {
		seen[s] = struct{}{}
	}
	return len(seen)
}

type position struct {
	Symbol     string
	Side       Side
	EntryPrice float64
	Qty        float64
	MarginUSDT float64
	Leverage   int
	OpenedAt   time.Time
	MegaExit   bool
	PeakPrice  float64
	Partial    bool
}

func NewExecutor(cfg Config, client *binance.FuturesClient, journal *TradeJournal, focus *FocusController) *Executor {
	return &Executor{
		focus: focus,
		cfg:        cfg,
		client:     client,
		journal:    journal,
		lastTrade:  make(map[string]time.Time),
		active:     make(map[string]*position),
		dryOpen:    make(map[string]*simPosition),
		dryPartial: make(map[string]float64),
		reverseLive: make(map[string]*reversePosition),
		closing:     make(map[string]bool),
		revPending:   make(map[string]*reverseLimitPending),
		dryRevOpen:   make(map[string]*simPosition),
		revClosing:   make(map[string]bool),
		lastRevTrade: make(map[string]time.Time),
		revLimitLivePending: make(map[string]*reverseLimitLivePending),
		revLimitLiveOpen:    make(map[string]*earlyLivePosition),
		lastRevLiveTrade:    make(map[string]time.Time),
		samePending:          make(map[string]*reverseLimitPending),
		lastDrySameTrade:     make(map[string]time.Time),
		sameLimitLivePending: make(map[string]*reverseLimitLivePending),
		lastSameLimitLiveTrade: make(map[string]time.Time),
	}
}

func (e *Executor) HandleSignal(ctx context.Context, sig *Signal) {
	if sig == nil {
		return
	}
	if sig.Kind != SignalEarly && !e.cfg.AllowsSignalSide(sig.Side) {
		return
	}
	sym := sig.Symbol
	if e.cfg.IsSymbolBlocked(sym) {
		return
	}
	if !e.cfg.DryRun && !e.client.SymbolTradable(sym) {
		return
	}

	e.mu.Lock()
	if e.countOpenSlotsLocked() >= e.cfg.MaxOpenPositions {
		e.mu.Unlock()
		return
	}
	if t, ok := e.lastTrade[sym]; ok && time.Since(t) < e.cfg.Cooldown() {
		e.mu.Unlock()
		return
	}
	if e.cfg.DryRun {
		if _, busy := e.dryOpen[sym]; busy {
			e.mu.Unlock()
			return
		}
		if e.cfg.ReverseLive {
			if _, live := e.reverseLive[sym]; live {
				e.mu.Unlock()
				return
			}
		}
	} else if _, busy := e.active[sym]; busy {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	if e.focus != nil && !e.focus.TryBegin(sym) {
		return
	}
	opened := false
	defer func() {
		if !opened && e.focus != nil {
			e.focus.ReleaseWithoutTrade(sym)
		}
	}()

	margin, lev := e.marginAndLeverage(sig)
	if sig.Kind == SignalEarly && (e.cfg.Early.DrySameEnabled || e.cfg.EarlyVol3SameMode()) {
		margin, lev = e.marginForEarlyPath(sig, earlyDrySame)
	}
	tradeSide := e.cfg.TradeSide(sig.Side)
	side := string(tradeSide)
	simMode := e.cfg.DrySimMode
	if simMode == "" {
		simMode = "tick"
	}

	if e.cfg.DryRun {
		entry := e.simEntryPrice(sig, tradeSide)
		if entry <= 0 {
			log.Printf("[whale] SIGNAL skip %s %s: no entry price", side, sym)
			return
		}
		opened = true
		e.logSignalEntry(sig, tradeSide, entry, margin, lev)
		if e.journal != nil {
			e.journal.LogEntry(sig, tradeSide, entry, margin, lev, simMode)
		}
		absMove := sig.MovePct
		if absMove < 0 {
			absMove = -absMove
		}
		megaExit := sig.Mega || sig.Kind == SignalBurst
		if sig.Kind == SignalEarly {
			megaExit = false
		}
		sim := &simPosition{
			Symbol: sym, Side: tradeSide, EntryPrice: entry, MarginUSDT: margin, Leverage: lev,
			OpenedAt: time.Now(), MegaExit: megaExit,
			PeakPrice: entry, LastPrice: entry, SignalAbsMovePct: absMove,
			SignalKind: sig.Kind,
		}
		if sig.Kind == SignalEarly && !sig.RecvAt.IsZero() {
			sim.ScheduledExitAt = e.earlyScheduledExitAt(sig.RecvAt)
		}
		e.mu.Lock()
		e.dryOpen[sym] = sim
		e.trackDryOpen(sym)
		e.openCount++
		e.lastTrade[sym] = time.Now()
		e.mu.Unlock()
		if e.cfg.ReverseLive {
			go e.openReverseLive(sig, entry)
		}
		if sig.Kind == SignalEarly && e.cfg.EarlyLiveTrade {
			go e.openEarlyLive(sig, entry)
		}
		go e.dryRunTimeout(ctx, sym, sig.Kind)
		go e.manageDryRunMarkPoll(ctx, sym, sig.Kind)
		return
	}

	effLev := e.client.EffectiveLeverage(sym, lev)
	if err := e.client.SetLeverage(sym, effLev); err != nil {
		log.Printf("[whale] set leverage %s %dx: %v", sym, effLev, err)
	}
	notional := margin * float64(effLev)
	start := time.Now()
	resp, err := e.client.MarketOrder(sym, side, notional)
	execMs := time.Since(start)
	if err != nil {
		log.Printf("[whale] order failed %s %s: %v (%s)", side, sym, err, execMs)
		return
	}
	entry, qty := parseFill(resp)
	e.logSignalEntry(sig, tradeSide, entry, margin, effLev)

	if sig.Kind == SignalEarly {
		scheduled := time.Time{}
		if !sig.RecvAt.IsZero() {
			scheduled = e.earlyScheduledExitAt(sig.RecvAt)
		}
		ep := &earlyLivePosition{
			Symbol: sym, Side: tradeSide, EntryPrice: entry, Qty: qty,
			MarginUSDT: margin, Leverage: effLev, OpenedAt: time.Now(),
			ScheduledExitAt: scheduled,
		}
		e.mu.Lock()
		e.earlyLive = e.ensureEarlyLiveMap()
		e.earlyLive[sym] = ep
		e.openCount++
		e.lastTrade[sym] = time.Now()
		e.mu.Unlock()
		if e.journal != nil {
			e.journal.LogLiveEntry(sig, tradeSide, sig.EntryPrice, entry, margin, effLev)
		}
		opened = true
		go e.manageEarlyLiveExit(sym)
		return
	}

	if e.journal != nil {
		e.journal.LogEntry(sig, tradeSide, entry, margin, effLev, "live")
	}
	pos := &position{
		Symbol: sym, Side: tradeSide, EntryPrice: entry, Qty: qty, MarginUSDT: margin, Leverage: effLev,
		OpenedAt: time.Now(), MegaExit: sig.Mega || sig.Kind == SignalBurst, PeakPrice: entry,
	}
	e.mu.Lock()
	e.active[sym] = pos
	e.openCount++
	e.lastTrade[sym] = time.Now()
	e.mu.Unlock()

	opened = true
	go e.manageExit(ctx, pos)
}

func (e *Executor) notifyTradeClosed(sym string) {
	if e.focus != nil {
		e.focus.EndTradeCooldown(sym)
	}
}

// HasDryPosition reports whether tick-based exit should run for this symbol.
func (e *Executor) HasDryPosition(sym string) bool {
	_, ok := e.dryOpenSyms.Load(sym)
	return ok
}

func (e *Executor) trackDryOpen(sym string) { e.dryOpenSyms.Store(sym, struct{}{}) }
func (e *Executor) untrackDryOpen(sym string) { e.dryOpenSyms.Delete(sym) }

// OnPriceTick updates open dry positions from live aggTrade (tick sim mode).
func (e *Executor) OnPriceTick(sym string, price float64, at time.Time) {
	if !e.cfg.DryRun || !e.cfg.UsesTickDrySim() || price <= 0 {
		return
	}
	e.mu.Lock()
	if e.closing[sym] {
		e.mu.Unlock()
		return
	}
	pos, ok := e.dryOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	px := applySlippage(price, pos.Side, e.cfg.DryExitSlippageBps, false)
	pos.LastPrice = px
	e.mu.Unlock()

	_ = e.dryExitStep(pos, px, at)
}

func (e *Executor) simEntryPrice(sig *Signal, tradeSide Side) float64 {
	entry := sig.EntryPrice
	if entry <= 0 {
		var err error
		entry, err = e.client.MarkPrice(sig.Symbol)
		if err != nil || entry <= 0 {
			return 0
		}
	}
	return applySlippage(entry, tradeSide, e.cfg.DryEntrySlippageBps, true)
}

func (e *Executor) marginAndLeverage(sig *Signal) (margin float64, leverage int) {
	if e.cfg.MarginUSDT > 0 {
		margin = e.cfg.MarginUSDT
	} else {
		capital := e.cfg.CapitalUSDT
		var pct float64
		if e.cfg.AllocationPercent > 0 {
			pct = e.cfg.AllocationPercent / 100
		} else if sig.Mega {
			pct = e.cfg.Risk.MegaRiskPercent / 100
		} else {
			pct = e.cfg.Risk.NormalRiskPercent / 100
		}
		maxPct := e.cfg.Risk.MaxPositionPercent / 100
		if pct > maxPct {
			pct = maxPct
		}
		margin = capital * pct
	}
	lev := e.cfg.Leverage
	if lev <= 0 {
		lev = 1
	}
	return margin, lev
}

func (e *Executor) logSignalEntry(sig *Signal, tradeSide Side, entry, margin float64, leverage int) {
	reverseNote := ""
	if e.cfg.ReverseTrade && tradeSide != sig.Side {
		reverseNote = fmt.Sprintf(" → trade %s", tradeSide)
	}
	switch sig.Kind {
	case SignalBookLead:
		log.Printf("[whale] SIGNAL %s %s%s book mode=%s imb=%.2fx flow=$%.0f move=%.2f%% entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, reverseNote, sig.BookMode, sig.ImbalanceRatio, sig.TradeFlowUSDT, sig.MovePct, entry, margin, leverage)
	case SignalBurst:
		log.Printf("[whale] SIGNAL %s %s%s BURST fast=%.2f%% 1s=%.2f%% vol=$%.0f entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, reverseNote, sig.FastMove, sig.MovePct, sig.SecVolume, entry, margin, leverage)
	case SignalEarly:
		log.Printf("[whale] SIGNAL %s %s%s EARLY r60=%.2f%% entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, reverseNote, sig.MovePct, entry, margin, leverage)
	default:
		log.Printf("[whale] SIGNAL %s %s%s flash mode=%s 1s=%.2f%% entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, reverseNote, sig.FlashMode, sig.MovePct, entry, margin, leverage)
	}
}

func (e *Executor) logExit(pos *simPosition, exitPrice float64, at time.Time, reason string, partialAlready float64) {
	pnl := closeSimPnL(e.cfg.RiskForExit(), pos, exitPrice)
	if partialAlready > 0 {
		pnl += partialAlready
	}
	ch := priceChangePct(pos.Side, pos.EntryPrice, exitPrice)
	log.Printf("[whale] EXIT %s %s reason=%s exit=%.6f pnl=%+.2f USDT (%+.2f%%) lev=%dx hold=%s",
		pos.Side, pos.Symbol, reason, exitPrice, pnl, ch, pos.Leverage, at.Sub(pos.OpenedAt).Round(time.Second))
}

func parseFill(resp map[string]any) (price, qty float64) {
	if resp == nil {
		return 0, 0
	}
	if ap, ok := resp["avgPrice"].(string); ok {
		fmt.Sscanf(ap, "%f", &price)
	}
	if q, ok := resp["executedQty"].(string); ok {
		fmt.Sscanf(q, "%f", &qty)
	}
	if price <= 0 {
		if ap, ok := resp["avgPrice"].(float64); ok {
			price = ap
		}
	}
	return price, qty
}

func (e *Executor) dryHoldDuration(kind SignalKind) time.Duration {
	if kind == SignalEarly {
		if m := e.cfg.Early.HoldMinutes; m > 0 {
			return time.Duration(m) * time.Minute
		}
		return 30 * time.Minute
	}
	return 10 * time.Minute
}

func (e *Executor) dryRunTimeout(ctx context.Context, sym string, kind SignalKind) {
	var wait time.Duration
	if kind == SignalEarly {
		e.mu.Lock()
		pos, ok := e.dryOpen[sym]
		if ok {
			wait = time.Until(e.earlyExitDeadline(pos))
		} else {
			wait = e.dryHoldDuration(kind)
		}
		e.mu.Unlock()
	} else {
		wait = e.dryHoldDuration(kind)
	}
	if wait < 0 {
		wait = 0
	}
	select {
	case <-ctx.Done():
		e.closeDry(sym, "ctx")
	case <-time.After(wait):
		reason := "timeout"
		if kind == SignalEarly {
			reason = "early_hold"
		}
		e.closeDry(sym, reason)
	}
}

func (e *Executor) claimDryExit(sym string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing[sym] {
		return false
	}
	if _, ok := e.dryOpen[sym]; !ok {
		return false
	}
	e.closing[sym] = true
	return true
}

func (e *Executor) finishDryClose(sym string) {
	e.mu.Lock()
	delete(e.dryOpen, sym)
	e.untrackDryOpen(sym)
	delete(e.dryPartial, sym)
	e.openCount--
	e.mu.Unlock()
}

func (e *Executor) closeDry(sym, reason string) {
	e.mu.Lock()
	if e.closing[sym] {
		e.mu.Unlock()
		return
	}
	pos, ok := e.dryOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	e.closing[sym] = true
	part := e.dryPartial[sym]
	delete(e.dryOpen, sym)
	e.untrackDryOpen(sym)
	delete(e.dryPartial, sym)
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
	e.logExit(pos, exit, at, reason, part)
	if e.journal != nil {
		e.journal.LogExit(e.cfg.RiskForExit(), pos, exit, at, reason, part)
	}
	if e.cfg.ReverseLive {
		e.closeReverseLive(sym, exit, reason, at)
	}
	if e.cfg.EarlyLiveTrade {
		e.closeEarlyLive(sym, reason, at)
	}
	e.notifyTradeClosed(sym)
}

func (e *Executor) manageDryRunMarkPoll(ctx context.Context, sym string, kind SignalKind) {
	hold := e.dryHoldDuration(kind)
	wait := hold
	if kind == SignalEarly {
		e.mu.Lock()
		if pos, ok := e.dryOpen[sym]; ok {
			wait = time.Until(e.earlyExitDeadline(pos))
		}
		e.mu.Unlock()
	}
	if wait < 0 {
		wait = 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			e.closeDry(sym, "ctx")
			return
		case <-timer.C:
			reason := "timeout"
			if kind == SignalEarly {
				reason = "early_hold"
			}
			e.closeDry(sym, reason)
			return
		case <-tick.C:
			e.mu.Lock()
			if e.closing[sym] {
				e.mu.Unlock()
				return
			}
			pos, ok := e.dryOpen[sym]
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
			if e.closing[sym] {
				e.mu.Unlock()
				return
			}
			pos, ok = e.dryOpen[sym]
			if !ok {
				e.mu.Unlock()
				return
			}
			px := applySlippage(mp, pos.Side, e.cfg.DryExitSlippageBps, false)
			pos.LastPrice = px
			e.mu.Unlock()

			if e.dryExitStep(pos, px, time.Now()) {
				return
			}
		}
	}
}

func (e *Executor) dryExitStep(pos *simPosition, price float64, at time.Time) bool {
	if pos.SignalKind == SignalEarly {
		if e.earlyTakeProfitHit(pos.Side, pos.EntryPrice, price) {
			if !e.claimDryExit(pos.Symbol) {
				return true
			}
			e.logExit(pos, price, at, "early_tp", 0)
			if e.journal != nil {
				e.journal.LogExit(e.cfg.RiskForExit(), pos, price, at, "early_tp", 0)
			}
			if e.cfg.EarlyLiveTrade {
				e.closeEarlyLive(pos.Symbol, "early_tp", at)
			}
			e.notifyTradeClosed(pos.Symbol)
			e.finishDryClose(pos.Symbol)
			return true
		}
		if !at.Before(e.earlyExitDeadline(pos)) {
			if !e.claimDryExit(pos.Symbol) {
				return true
			}
			e.logExit(pos, price, at, "early_hold", 0)
			if e.journal != nil {
				e.journal.LogExit(e.cfg.RiskForExit(), pos, price, at, "early_hold", 0)
			}
			if e.cfg.EarlyLiveTrade {
				e.closeEarlyLive(pos.Symbol, "early_hold", at)
			}
			e.notifyTradeClosed(pos.Symbol)
			e.finishDryClose(pos.Symbol)
			return true
		}
		return false
	}
	var closed bool
	var reason string
	var partial float64
	closed, reason, partial = positionExitStep(e.cfg, pos, price, at)
	if partial > 0 && !pos.Partial {
		pos.Partial = true
		e.mu.Lock()
		e.dryPartial[pos.Symbol] += partial
		e.mu.Unlock()
		if e.cfg.ReverseLive {
			e.reduceReverseLive(pos.Symbol, e.cfg.Risk.PartialExitFraction)
		}
		if e.journal != nil {
			e.journal.LogPartial(pos.Symbol, pos.Side, partial, at)
		}
	}
	if closed {
		if !e.claimDryExit(pos.Symbol) {
			return true
		}
		part := 0.0
		e.mu.Lock()
		part = e.dryPartial[pos.Symbol]
		e.mu.Unlock()
		e.logExit(pos, price, at, reason, part)
		if e.journal != nil {
			e.journal.LogExit(e.cfg.RiskForExit(), pos, price, at, reason, part)
		}
		if e.cfg.ReverseLive {
			e.closeReverseLive(pos.Symbol, price, reason, at)
		}
		e.notifyTradeClosed(pos.Symbol)
		e.finishDryClose(pos.Symbol)
		return true
	}
	return false
}

func (e *Executor) manageExit(ctx context.Context, pos *position) {
	defer func() {
		e.mu.Lock()
		delete(e.active, pos.Symbol)
		e.openCount--
		e.mu.Unlock()
	}()

	if pos.EntryPrice <= 0 || pos.Qty <= 0 {
		return
	}

	if pos.MegaExit {
		e.manageMegaExit(ctx, pos)
		return
	}
	e.manageStandardExit(ctx, pos)
}

func (e *Executor) manageMegaExit(ctx context.Context, pos *position) {
	r := e.cfg.Risk
	closeSide := "SELL"
	if pos.Side == SideSell {
		closeSide = "BUY"
	}
	partialFrac := r.PartialExitFraction
	remaining := pos.Qty
	partialDone := false
	var partialUSDT float64

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(10 * time.Minute)

	closeRemaining := func(reason string) {
		mp, _ := e.client.MarkPrice(pos.Symbol)
		if mp <= 0 {
			mp = pos.EntryPrice
		}
		if remaining > 0 {
			e.closeAll(pos.Symbol, closeSide, remaining)
		}
		sim := liveSimFrom(pos, partialDone)
		e.logExit(sim, mp, time.Now(), reason, partialUSDT)
		if e.journal != nil {
			e.journal.LogExit(r, sim, mp, time.Now(), reason, partialUSDT)
		}
	}

	for {
		select {
		case <-ctx.Done():
			closeRemaining("ctx")
			return
		case <-deadline:
			closeRemaining("timeout")
			return
		case <-tick.C:
			mp, err := e.client.MarkPrice(pos.Symbol)
			if err != nil || mp <= 0 {
				continue
			}
			sim := liveSimFrom(pos, partialDone)
			closed, reason, partialPnL := positionExitStep(e.cfg, sim, mp, time.Now())
			pos.PeakPrice = sim.PeakPrice
			if partialPnL > 0 && !partialDone {
				half := remaining * partialFrac
				_, _ = e.client.MarketOrderQty(pos.Symbol, closeSide, half)
				remaining -= half
				partialDone = true
				pos.Partial = true
				partialUSDT += partialPnL
				if e.journal != nil {
					e.journal.LogPartial(pos.Symbol, pos.Side, partialPnL, time.Now())
				}
			}
			if closed {
				closeRemaining(reason)
				return
			}
		}
	}
}

func liveSimFrom(pos *position, partial bool) *simPosition {
	lev := pos.Leverage
	if lev <= 0 {
		lev = 1
	}
	return &simPosition{
		Symbol: pos.Symbol, Side: pos.Side, EntryPrice: pos.EntryPrice,
		MarginUSDT: pos.MarginUSDT, Leverage: lev, OpenedAt: pos.OpenedAt, MegaExit: pos.MegaExit,
		PeakPrice: pos.PeakPrice, Partial: partial,
	}
}

func (e *Executor) manageStandardExit(ctx context.Context, pos *position) {
	slPct := e.cfg.Risk.StopLossPercent / 100
	tp1Pct := e.cfg.Risk.TakeProfitPercent1 / 100
	tp2Pct := e.cfg.Risk.TakeProfitPercent2 / 100
	partial := e.cfg.Risk.PartialExitFraction

	var slPrice, tp1Price, tp2Price float64
	closeSide := "SELL"
	if pos.Side == SideBuy {
		slPrice = pos.EntryPrice * (1 - slPct)
		tp1Price = pos.EntryPrice * (1 + tp1Pct)
		tp2Price = pos.EntryPrice * (1 + tp2Pct)
	} else {
		slPrice = pos.EntryPrice * (1 + slPct)
		tp1Price = pos.EntryPrice * (1 - tp1Pct)
		tp2Price = pos.EntryPrice * (1 - tp2Pct)
		closeSide = "BUY"
	}

	halfQty := pos.Qty * partial
	remaining := pos.Qty
	tp1Done := false
	var partialUSDT float64

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(15 * time.Minute)

	finish := func(reason string) {
		mp, _ := e.client.MarkPrice(pos.Symbol)
		if mp <= 0 {
			mp = pos.EntryPrice
		}
		if remaining > 0 {
			e.closeAll(pos.Symbol, closeSide, remaining)
		}
		sim := liveSimFrom(pos, tp1Done)
		e.logExit(sim, mp, time.Now(), reason, partialUSDT)
		if e.journal != nil {
			e.journal.LogExit(e.cfg.RiskForExit(), sim, mp, time.Now(), reason, partialUSDT)
		}
	}

	for {
		select {
		case <-ctx.Done():
			finish("ctx")
			return
		case <-deadline:
			finish("timeout")
			return
		case <-tick.C:
			mp, err := e.client.MarkPrice(pos.Symbol)
			if err != nil || mp <= 0 {
				continue
			}
			if pos.Side == SideBuy {
				if mp <= slPrice {
					finish("sl")
					return
				}
				if !tp1Done && mp >= tp1Price {
					if halfQty > 0 {
						_, _ = e.client.MarketOrderQty(pos.Symbol, closeSide, halfQty)
						remaining -= halfQty
					}
					tp1Done = true
					partialUSDT += pos.NotionalUSDT() * partial * tp1Pct
				}
				if tp1Done && mp >= tp2Price {
					finish("tp2")
					return
				}
			} else {
				if mp >= slPrice {
					finish("sl")
					return
				}
				if !tp1Done && mp <= tp1Price {
					if halfQty > 0 {
						_, _ = e.client.MarketOrderQty(pos.Symbol, closeSide, halfQty)
						remaining -= halfQty
					}
					tp1Done = true
					partialUSDT += pos.NotionalUSDT() * partial * tp1Pct
				}
				if tp1Done && mp <= tp2Price {
					finish("tp2")
					return
				}
			}
		}
	}
}

func (p *position) NotionalUSDT() float64 {
	lev := p.Leverage
	if lev <= 0 {
		lev = 1
	}
	return p.MarginUSDT * float64(lev)
}

func (e *Executor) closeAll(symbol, side string, qty float64) {
	if qty <= 0 || e.cfg.DryRun {
		return
	}
	_, _ = e.client.MarketOrderQty(symbol, side, qty)
}
