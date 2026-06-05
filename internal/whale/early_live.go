package whale

import (
	"fmt"
	"log"
	"time"
)

type earlyLivePosition struct {
	Symbol     string
	Side       Side
	EntryPrice float64
	Qty        float64
	MarginUSDT float64
	Leverage   int
	OpenedAt   time.Time
}

func (e *Executor) earlyLeverage(sym string) int {
	cap := e.cfg.MaxLeverageCap
	if cap <= 0 {
		cap = 50
	}
	lev := e.client.MaxLeverage(sym)
	if e.cfg.Leverage > 0 {
		lev = e.cfg.Leverage
	}
	if lev <= 0 {
		lev = cap
	}
	if lev > cap {
		lev = cap
	}
	if lev < 1 {
		lev = 1
	}
	return lev
}

func (e *Executor) earlyLiveSizing(sym string) (margin, notional float64, lev int, bal float64, err error) {
	bal, err = e.client.AvailableUSDTBalance()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("fetch balance: %w", err)
	}
	if bal <= 0 {
		return 0, 0, 0, bal, fmt.Errorf("available balance is zero")
	}
	lev = e.earlyLeverage(sym)
	if e.cfg.MarginUSDT > 0 {
		margin = e.cfg.MarginUSDT
	} else if e.cfg.AllocationPercent > 0 {
		margin = bal * (e.cfg.AllocationPercent / 100)
	} else {
		margin = e.cfg.CapitalUSDT * 0.25
	}
	notional = margin * float64(lev)
	const minNotional = 5.0
	if notional < minNotional {
		needMargin := minNotional / float64(lev)
		if needMargin <= margin || e.cfg.MarginUSDT > 0 {
			margin = needMargin
			notional = minNotional
		}
	}
	return margin, notional, lev, bal, nil
}

func (e *Executor) openEarlyLive(sig *Signal, signalEntry float64) {
	if !e.cfg.EarlyLiveTrade || !e.client.Configured() {
		return
	}
	sym := sig.Symbol
	side := sig.Side
	if e.cfg.DryRun && !e.dryStillOpen(sym) {
		log.Printf("[early] live skip %s: dry position already closed", sym)
		return
	}

	lev := e.earlyLeverage(sym)
	if err := e.client.SetLeverage(sym, lev); err != nil {
		log.Printf("[early] live set leverage %s %dx: %v", sym, lev, err)
	}
	margin, notional, lev, bal, err := e.earlyLiveSizing(sym)
	if err != nil {
		log.Printf("[early] live skip %s: %v", sym, err)
		return
	}
	if notional < 5 {
		log.Printf("[early] live skip %s: notional %.2f < 5 USDT (bal=%.2f)", sym, notional, bal)
		return
	}

	orderSide := "BUY"
	if side == SideSell {
		orderSide = "SELL"
	}
	start := time.Now()
	resp, err := e.client.MarketOrder(sym, orderSide, notional)
	if err != nil {
		log.Printf("[early] live order failed %s %s: %v (%s)", orderSide, sym, err, time.Since(start))
		return
	}
	entry, qty := parseFill(resp)
	if entry <= 0 {
		entry = signalEntry
	}

	ep := &earlyLivePosition{
		Symbol: sym, Side: side, EntryPrice: entry, Qty: qty,
		MarginUSDT: margin, Leverage: lev, OpenedAt: time.Now(),
	}
	e.mu.Lock()
	e.earlyLive = e.ensureEarlyLiveMap()
	e.earlyLive[sym] = ep
	e.mu.Unlock()

	log.Printf("[early] LIVE ENTRY %s %s fill=%.6f margin=%.2f lev=%dx notional=%.2f bal=%.2f",
		side, sym, entry, margin, lev, notional, bal)
	if e.journal != nil {
		e.journal.LogLiveEntry(sig, side, signalEntry, entry, margin, lev)
	}
	go e.manageEarlyLiveExit(sym)
}

func (e *Executor) ensureEarlyLiveMap() map[string]*earlyLivePosition {
	if e.earlyLive == nil {
		e.earlyLive = make(map[string]*earlyLivePosition)
	}
	return e.earlyLive
}

func (e *Executor) manageEarlyLiveExit(sym string) {
	hold := time.Duration(e.cfg.Early.HoldMinutes) * time.Minute
	if hold <= 0 {
		hold = 30 * time.Minute
	}
	time.Sleep(hold)
	e.closeEarlyLive(sym, "early_hold", time.Now())
}

func (e *Executor) closeEarlyLive(sym, reason string, at time.Time) {
	e.mu.Lock()
	if e.earlyLive == nil {
		e.mu.Unlock()
		return
	}
	ep, ok := e.earlyLive[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	delete(e.earlyLive, sym)
	e.mu.Unlock()

	closeSide := "SELL"
	if ep.Side == SideSell {
		closeSide = "BUY"
	}
	if ep.Qty > 0 {
		if _, err := e.client.MarketOrderQty(sym, closeSide, ep.Qty); err != nil {
			log.Printf("[early] live close failed %s %s: %v", closeSide, sym, err)
		}
	}
	exitPx, _ := e.client.MarkPrice(sym)
	if exitPx <= 0 {
		exitPx = ep.EntryPrice
	}
	pnl := livePnL(ep.Side, ep.EntryPrice, exitPx, ep.MarginUSDT, ep.Leverage)
	ch := priceChangePct(ep.Side, ep.EntryPrice, exitPx)
	log.Printf("[early] LIVE EXIT %s %s reason=%s exit=%.6f pnl=%+.2f USDT hold=%s",
		ep.Side, sym, reason, exitPx, pnl, at.Sub(ep.OpenedAt).Round(time.Second))
	if e.journal != nil {
		rp := &reversePosition{Symbol: sym, RealSide: ep.Side, EntryPrice: ep.EntryPrice, OpenedAt: ep.OpenedAt}
		e.journal.LogLiveExit(rp, exitPx, exitPx, at, reason, pnl, ch)
	}
}

func livePnL(side Side, entry, exit, margin float64, lev int) float64 {
	ch := priceChangePct(side, entry, exit)
	return margin * float64(lev) * ch / 100
}
