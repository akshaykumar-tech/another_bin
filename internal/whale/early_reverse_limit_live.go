package whale

import (
	"context"
	"fmt"
	"log"
	"time"

	"crypto_announcements_go/internal/binance"
)

type reverseLimitLivePending struct {
	Symbol     string
	TradeSide  Side
	LimitPx    float64
	OrderID    int64
	SignalAt   time.Time
	EntryRefAt time.Time
	ExpiresAt  time.Time
	Margin     float64
	Leverage   int
	Sig        Signal
}

func (e *Executor) initRevLimitLiveMaps() {
	if e.revLimitLivePending == nil {
		e.revLimitLivePending = make(map[string]*reverseLimitLivePending)
	}
	if e.revLimitLiveOpen == nil {
		e.revLimitLiveOpen = make(map[string]*earlyLivePosition)
	}
	if e.lastRevLiveTrade == nil {
		e.lastRevLiveTrade = make(map[string]time.Time)
	}
}

func (e *Executor) earlyReverseLeverage(sym string) int {
	cap := e.cfg.MaxLeverageCap
	if cap <= 0 {
		cap = 50
	}
	lev := e.cfg.EarlyReverseLeverage
	if lev <= 0 {
		lev = e.cfg.Leverage
	}
	if lev <= 0 {
		lev = e.client.MaxLeverage(sym)
	}
	lev = e.client.EffectiveLeverage(sym, lev)
	if lev > cap {
		lev = cap
	}
	if lev < 1 {
		lev = 1
	}
	return lev
}

func (e *Executor) earlyReverseLiveSizing(sym string) (margin, notional float64, lev int, bal float64, err error) {
	bal, err = e.client.AvailableUSDTBalance()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("fetch balance: %w", err)
	}
	if bal <= 0 {
		return 0, 0, 0, bal, fmt.Errorf("available balance is zero")
	}
	lev = e.earlyReverseLeverage(sym)
	switch {
	case e.cfg.EarlyReverseMarginUSDT > 0:
		margin = e.cfg.EarlyReverseMarginUSDT
	case e.cfg.EarlyReverseAllocationPercent > 0:
		margin = bal * (e.cfg.EarlyReverseAllocationPercent / 100)
	case e.cfg.MarginUSDT > 0:
		margin = e.cfg.MarginUSDT
	case e.cfg.AllocationPercent > 0:
		margin = bal * (e.cfg.AllocationPercent / 100)
	default:
		margin = e.cfg.CapitalUSDT * 0.25
	}
	notional = margin * float64(lev)
	const minNotional = 5.0
	if notional < minNotional {
		needMargin := minNotional / float64(lev)
		if needMargin <= margin || e.cfg.EarlyReverseMarginUSDT > 0 || e.cfg.MarginUSDT > 0 {
			margin = needMargin
			notional = minNotional
		}
	}
	return margin, notional, lev, bal, nil
}

func (e *Executor) canStartRevLimitLive(sym string, at time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initRevLimitLiveMaps()
	if e.countOpenSlotsLocked() >= e.cfg.MaxOpenPositions {
		return false
	}
	if _, open := e.revLimitLiveOpen[sym]; open {
		return false
	}
	if _, pend := e.revLimitLivePending[sym]; pend {
		return false
	}
	if t, ok := e.lastRevLiveTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		return false
	}
	return true
}

func (e *Executor) TryStartReverseLimitLive(ctx context.Context, sig *Signal, volAccel float64) {
	if sig == nil || !e.cfg.Early.LiveReverseLimitEnabled || !e.client.Configured() {
		return
	}
	minVol := e.cfg.Early.MinVolAccel
	if minVol > 0 && volAccel < minVol {
		return
	}
	sym := sig.Symbol
	if e.cfg.IsSymbolBlocked(sym) || !e.client.SymbolTradable(sym) {
		return
	}
	at := sig.RecvAt
	if at.IsZero() {
		at = time.Now()
	}
	if !e.canStartRevLimitLive(sym, at) {
		return
	}

	tradeSide := oppositeSide(sig.Side)
	limitPx := EarlySameDirLimitPxFromSignal(e.cfg, sig)
	if limitPx <= 0 {
		return
	}

	lev := e.earlyReverseLeverage(sym)
	if err := e.client.SetLeverage(sym, lev); err != nil {
		log.Printf("[early] rev live set leverage %s %dx: %v", sym, lev, err)
	}
	margin, notional, lev, bal, err := e.earlyReverseLiveSizing(sym)
	if err != nil {
		log.Printf("[early] rev live skip %s: %v", sym, err)
		return
	}
	if notional < 5 {
		log.Printf("[early] rev live skip %s: notional %.2f < 5 USDT (bal=%.2f)", sym, notional, bal)
		return
	}

	orderSide := "BUY"
	if tradeSide == SideSell {
		orderSide = "SELL"
	}
	start := time.Now()
	resp, err := e.client.LimitOrderGTX(sym, orderSide, limitPx, notional)
	if err != nil {
		log.Printf("[early] REV_LIVE limit rejected %s %s @ %.6f: %v (%s)", orderSide, sym, limitPx, err, time.Since(start))
		return
	}
	orderID := orderIDFromResp(resp)
	if orderID <= 0 {
		log.Printf("[early] rev live skip %s: no order id in response", sym)
		return
	}

	window := e.limitFillWindow()
	entryRefAt := at.Add(EarlyEntryDelay(e.cfg))
	expires := entryRefAt.Add(window)
	pend := &reverseLimitLivePending{
		Symbol: sym, TradeSide: tradeSide, LimitPx: limitPx, OrderID: orderID,
		SignalAt: at, EntryRefAt: entryRefAt, ExpiresAt: expires, Margin: margin, Leverage: lev,
		Sig: *sig,
	}
	e.mu.Lock()
	e.initRevLimitLiveMaps()
	e.revLimitLivePending[sym] = pend
	e.lastRevLiveTrade[sym] = at
	e.mu.Unlock()

	log.Printf("[early] REV_LIVE limit posted %s %s id=%d limit=%.6f vol=%.1fx window=%s margin=%.2f lev=%dx",
		tradeSide, sym, orderID, limitPx, volAccel, window.Round(time.Second), margin, lev)

	if st, _ := resp["status"].(string); st == "FILLED" {
		fillSt := binance.OrderStatus{Status: st}
		fillSt.AvgPrice, fillSt.ExecutedQty = parseFill(resp)
		go e.onRevLimitLiveFilled(sym, pend, fillSt)
		return
	}
	go e.watchReverseLimitLiveOrder(ctx, sym, pend)
}

func orderIDFromResp(m map[string]any) int64 {
	if m == nil {
		return 0
	}
	if v, ok := m["orderId"].(float64); ok {
		return int64(v)
	}
	return 0
}

func (e *Executor) watchReverseLimitLiveOrder(ctx context.Context, sym string, pend *reverseLimitLivePending) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		wait := time.Until(pend.ExpiresAt)
		if wait <= 0 {
			e.expireRevLimitLivePending(sym, pend)
			return
		}

		select {
		case <-ctx.Done():
			e.cancelRevLimitLivePending(sym, pend, "ctx")
			return
		case <-time.After(wait):
			e.expireRevLimitLivePending(sym, pend)
			return
		case <-ticker.C:
			st, err := e.client.QueryOrder(sym, pend.OrderID)
			if err != nil {
				log.Printf("[early] rev live poll %s id=%d: %v", sym, pend.OrderID, err)
				continue
			}
			switch st.Status {
			case "FILLED":
				e.onRevLimitLiveFilled(sym, pend, st)
				return
			case "CANCELED", "EXPIRED", "REJECTED":
				e.clearRevLimitLivePending(sym)
				log.Printf("[early] REV_LIVE order %s id=%d status=%s", sym, pend.OrderID, st.Status)
				return
			case "PARTIALLY_FILLED":
				if time.Now().After(pend.ExpiresAt) {
					e.onRevLimitLiveFilled(sym, pend, st)
					_ = e.client.CancelOrder(sym, pend.OrderID)
					return
				}
			}
		}
	}
}

func (e *Executor) clearRevLimitLivePending(sym string) {
	e.mu.Lock()
	delete(e.revLimitLivePending, sym)
	e.mu.Unlock()
}

func (e *Executor) cancelRevLimitLivePending(sym string, pend *reverseLimitLivePending, reason string) {
	if pend == nil {
		return
	}
	_ = e.client.CancelOrder(sym, pend.OrderID)
	e.clearRevLimitLivePending(sym)
	log.Printf("[early] REV_LIVE canceled %s id=%d reason=%s", sym, pend.OrderID, reason)
}

func (e *Executor) expireRevLimitLivePending(sym string, pend *reverseLimitLivePending) {
	if pend == nil {
		return
	}
	st, err := e.client.QueryOrder(sym, pend.OrderID)
	if err == nil && st.Status == "FILLED" {
		e.onRevLimitLiveFilled(sym, pend, st)
		return
	}
	if err == nil && st.ExecutedQty > 0 && (st.Status == "PARTIALLY_FILLED" || st.Status == "NEW") {
		e.onRevLimitLiveFilled(sym, pend, st)
		_ = e.client.CancelOrder(sym, pend.OrderID)
		return
	}
	_ = e.client.CancelOrder(sym, pend.OrderID)
	e.clearRevLimitLivePending(sym)
	log.Printf("[early] REV_LIVE expired %s id=%d (no fill)", sym, pend.OrderID)
}

func (e *Executor) onRevLimitLiveFilled(sym string, pend *reverseLimitLivePending, st binance.OrderStatus) {
	if pend == nil {
		return
	}
	entry := st.AvgPrice
	qty := st.ExecutedQty
	if entry <= 0 {
		entry = pend.LimitPx
	}
	if qty <= 0 {
		e.clearRevLimitLivePending(sym)
		log.Printf("[early] REV_LIVE fill %s id=%d but zero qty", sym, pend.OrderID)
		return
	}

	fillAt := time.Now()
	scheduledExit := e.earlyScheduledExitAt(pend.SignalAt)
	ep := &earlyLivePosition{
		Symbol: sym, Side: pend.TradeSide, EntryPrice: entry, Qty: qty,
		MarginUSDT: pend.Margin, Leverage: pend.Leverage, OpenedAt: fillAt,
		ScheduledExitAt: scheduledExit,
	}
	e.mu.Lock()
	e.initRevLimitLiveMaps()
	delete(e.revLimitLivePending, sym)
	e.revLimitLiveOpen[sym] = ep
	e.mu.Unlock()

	sig := pend.Sig
	log.Printf("[early] REV_LIVE fill %s %s @ %.6f qty=%.8f (signal %s) margin=%.2f lev=%dx exit_at=%s",
		pend.TradeSide, sym, entry, qty, sig.Side, pend.Margin, pend.Leverage, scheduledExit.Format("15:04:05"))
	if e.journal != nil {
		e.journal.LogLiveEntry(&sig, pend.TradeSide, pend.LimitPx, entry, pend.Margin, pend.Leverage)
	}
	if !fillAt.Before(scheduledExit) {
		e.closeRevLimitLive(sym, "early_hold", fillAt)
		return
	}
	go e.manageRevLimitLiveExit(sym, scheduledExit)
}

func (e *Executor) manageRevLimitLiveExit(sym string, deadline time.Time) {
	if !deadline.IsZero() {
		if wait := time.Until(deadline); wait > 0 {
			time.Sleep(wait)
		}
	}
	e.closeRevLimitLive(sym, "early_hold", time.Now())
}

func (e *Executor) closeRevLimitLive(sym, reason string, at time.Time) {
	e.mu.Lock()
	e.initRevLimitLiveMaps()
	ep, ok := e.revLimitLiveOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	delete(e.revLimitLiveOpen, sym)
	e.mu.Unlock()

	closeSide := "SELL"
	if ep.Side == SideSell {
		closeSide = "BUY"
	}
	if ep.Qty > 0 {
		if _, err := e.client.MarketOrderQty(sym, closeSide, ep.Qty); err != nil {
			log.Printf("[early] REV_LIVE close failed %s %s: %v", closeSide, sym, err)
		}
	}
	exitPx, _ := e.client.MarkPrice(sym)
	if exitPx <= 0 {
		exitPx = ep.EntryPrice
	}
	pnl := livePnL(ep.Side, ep.EntryPrice, exitPx, ep.MarginUSDT, ep.Leverage)
	ch := priceChangePct(ep.Side, ep.EntryPrice, exitPx)
	log.Printf("[early] REV_LIVE EXIT %s %s reason=%s exit=%.6f pnl=%+.2f USDT hold=%s",
		ep.Side, sym, reason, exitPx, pnl, at.Sub(ep.OpenedAt).Round(time.Second))
	if e.journal != nil {
		rp := &reversePosition{
			Symbol: sym, SignalSide: oppositeSide(ep.Side), RealSide: ep.Side,
			EntryPrice: ep.EntryPrice, OpenedAt: ep.OpenedAt, Leverage: ep.Leverage,
		}
		e.journal.LogLiveExit(rp, exitPx, exitPx, at, reason, pnl, ch)
	}
}
