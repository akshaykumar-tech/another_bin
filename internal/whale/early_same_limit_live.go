package whale

import (
	"context"
	"log"
	"time"

	"crypto_announcements_go/internal/binance"
)

func (e *Executor) initSameLimitLiveMaps() {
	if e.sameLimitLivePending == nil {
		e.sameLimitLivePending = make(map[string]*reverseLimitLivePending)
	}
	if e.lastSameLimitLiveTrade == nil {
		e.lastSameLimitLiveTrade = make(map[string]time.Time)
	}
}

// countEarlyLiveSlotsLocked counts open + pending same-dir Binance positions (caller must hold e.mu).
func (e *Executor) countEarlyLiveSlotsLocked() int {
	seen := make(map[string]struct{})
	if e.earlyLive != nil {
		for s := range e.earlyLive {
			seen[s] = struct{}{}
		}
	}
	if e.sameLimitLivePending != nil {
		for s := range e.sameLimitLivePending {
			seen[s] = struct{}{}
		}
	}
	return len(seen)
}

func (e *Executor) canStartSameLimitLive(sym string, at time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initSameLimitLiveMaps()
	if e.countEarlyLiveSlotsLocked() >= e.cfg.EarlyMaxOpenLivePositions() {
		return false
	}
	if e.earlyLive != nil {
		if _, open := e.earlyLive[sym]; open {
			return false
		}
	}
	if _, pend := e.sameLimitLivePending[sym]; pend {
		return false
	}
	if t, ok := e.lastSameLimitLiveTrade[sym]; ok && at.Sub(t) < e.cfg.Cooldown() {
		return false
	}
	return true
}

func (e *Executor) TryStartSameLimitLive(ctx context.Context, sig *Signal, volAccel float64) {
	if sig == nil || !e.cfg.EarlyLiveTrade || !e.client.Configured() {
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
	if !e.canStartSameLimitLive(sym, at) {
		return
	}

	tradeSide := e.cfg.TradeSide(sig.Side)
	signalLimit := EarlySameDirLimitPxFromSignal(e.cfg, sig)
	if signalLimit <= 0 {
		return
	}
	limitPx := e.earlyLiveGTXLimitPx(sym, tradeSide, signalLimit)
	if limitPx <= 0 {
		return
	}
	if limitPx != signalLimit {
		log.Printf("[early] SAME_LIVE gtx adjust %s %s limit %.6f -> %.6f (post-only maker)",
			tradeSide, sym, signalLimit, limitPx)
	}

	lev := e.earlyMaxLeverage(sym)
	if err := e.client.SetLeverage(sym, lev); err != nil {
		log.Printf("[early] same live set leverage %s %dx: %v", sym, lev, err)
	}
	margin, notional, lev, bal, err := e.earlySameLiveSizing(sym)
	if err != nil {
		log.Printf("[early] same live skip %s: %v", sym, err)
		return
	}
	if notional < 5 {
		log.Printf("[early] same live skip %s: notional %.2f < 5 USDT (bal=%.2f)", sym, notional, bal)
		return
	}

	e.mu.Lock()
	e.initSameLimitLiveMaps()
	if e.countEarlyLiveSlotsLocked() >= e.cfg.EarlyMaxOpenLivePositions() {
		e.mu.Unlock()
		return
	}
	if _, open := e.earlyLive[sym]; open {
		e.mu.Unlock()
		return
	}
	if _, pend := e.sameLimitLivePending[sym]; pend {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	orderSide := "BUY"
	if tradeSide == SideSell {
		orderSide = "SELL"
	}
	start := time.Now()
	resp, err := e.client.LimitOrderGTX(sym, orderSide, limitPx, notional)
	if err != nil {
		log.Printf("[early] SAME_LIVE limit rejected %s %s @ %.6f: %v (%s)", orderSide, sym, limitPx, err, time.Since(start))
		return
	}
	orderID := orderIDFromResp(resp)
	if orderID <= 0 {
		log.Printf("[early] same live skip %s: no order id in response", sym)
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
	e.initSameLimitLiveMaps()
	e.sameLimitLivePending[sym] = pend
	e.lastSameLimitLiveTrade[sym] = at
	e.mu.Unlock()

	log.Printf("[early] SAME_LIVE limit posted %s %s id=%d limit=%.6f notional=%.2f margin=%.4f lev=%dx vol=%.1fx window=%s bal=%.2f",
		tradeSide, sym, orderID, limitPx, notional, margin, lev, volAccel, window.Round(time.Second), bal)

	if st, _ := resp["status"].(string); st == "FILLED" {
		fillSt := binance.OrderStatus{Status: st}
		fillSt.AvgPrice, fillSt.ExecutedQty = parseFill(resp)
		go e.onSameLimitLiveFilled(sym, pend, fillSt)
		return
	}
	go e.watchSameLimitLiveOrder(ctx, sym, pend)
}

func (e *Executor) clearSameLimitLivePending(sym string) {
	e.mu.Lock()
	delete(e.sameLimitLivePending, sym)
	e.mu.Unlock()
}

func (e *Executor) cancelSameLimitLivePending(sym string, pend *reverseLimitLivePending, reason string) {
	if pend == nil {
		return
	}
	_ = e.client.CancelOrder(sym, pend.OrderID)
	e.clearSameLimitLivePending(sym)
	log.Printf("[early] SAME_LIVE canceled %s id=%d reason=%s", sym, pend.OrderID, reason)
}

func (e *Executor) expireSameLimitLivePending(sym string, pend *reverseLimitLivePending) {
	if pend == nil {
		return
	}
	st, err := e.client.QueryOrder(sym, pend.OrderID)
	if err == nil && st.Status == "FILLED" {
		e.onSameLimitLiveFilled(sym, pend, st)
		return
	}
	if err == nil && st.ExecutedQty > 0 && (st.Status == "PARTIALLY_FILLED" || st.Status == "NEW") {
		e.onSameLimitLiveFilled(sym, pend, st)
		_ = e.client.CancelOrder(sym, pend.OrderID)
		return
	}
	_ = e.client.CancelOrder(sym, pend.OrderID)
	e.clearSameLimitLivePending(sym)
	log.Printf("[early] SAME_LIVE expired %s id=%d (no fill)", sym, pend.OrderID)
}

func (e *Executor) watchSameLimitLiveOrder(ctx context.Context, sym string, pend *reverseLimitLivePending) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		wait := time.Until(pend.ExpiresAt)
		if wait <= 0 {
			e.expireSameLimitLivePending(sym, pend)
			return
		}

		select {
		case <-ctx.Done():
			e.cancelSameLimitLivePending(sym, pend, "ctx")
			return
		case <-time.After(wait):
			e.expireSameLimitLivePending(sym, pend)
			return
		case <-ticker.C:
			st, err := e.client.QueryOrder(sym, pend.OrderID)
			if err != nil {
				log.Printf("[early] same live poll %s id=%d: %v", sym, pend.OrderID, err)
				continue
			}
			switch st.Status {
			case "FILLED":
				e.onSameLimitLiveFilled(sym, pend, st)
				return
			case "CANCELED", "EXPIRED", "REJECTED":
				e.clearSameLimitLivePending(sym)
				log.Printf("[early] SAME_LIVE order %s id=%d status=%s", sym, pend.OrderID, st.Status)
				return
			case "PARTIALLY_FILLED":
				if time.Now().After(pend.ExpiresAt) {
					e.onSameLimitLiveFilled(sym, pend, st)
					_ = e.client.CancelOrder(sym, pend.OrderID)
					return
				}
			}
		}
	}
}

func (e *Executor) onSameLimitLiveFilled(sym string, pend *reverseLimitLivePending, st binance.OrderStatus) {
	if pend == nil {
		return
	}
	entry := st.AvgPrice
	qty := st.ExecutedQty
	if entry <= 0 {
		entry = pend.LimitPx
	}
	if qty <= 0 {
		e.clearSameLimitLivePending(sym)
		log.Printf("[early] SAME_LIVE fill %s id=%d but zero qty", sym, pend.OrderID)
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
	e.initSameLimitLiveMaps()
	delete(e.sameLimitLivePending, sym)
	e.earlyLive = e.ensureEarlyLiveMap()
	e.earlyLive[sym] = ep
	e.openCount++
	e.mu.Unlock()

	sig := pend.Sig
	log.Printf("[early] SAME_LIVE fill %s %s @ %.6f qty=%.8f margin=%.2f lev=%dx exit_at=%s",
		pend.TradeSide, sym, entry, qty, pend.Margin, pend.Leverage, scheduledExit.Format("15:04:05"))
	if e.journal != nil {
		e.journal.LogLiveEntry(&sig, pend.TradeSide, pend.LimitPx, entry, pend.Margin, pend.Leverage)
	}
	if !fillAt.Before(scheduledExit) {
		e.closeEarlyLive(sym, "early_hold", fillAt)
		return
	}
	go e.manageEarlyLiveExit(sym)
}
