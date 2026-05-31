package whale

import (
	"fmt"
	"log"
	"time"
)

type reversePosition struct {
	Symbol      string
	SignalSide  Side
	RealSide    Side
	SignalEntry float64
	EntryPrice  float64
	Qty         float64
	MarginUSDT  float64
	Leverage    int
	OpenedAt    time.Time
	Partial     bool
}

func oppositeSide(s Side) Side {
	if s == SideBuy {
		return SideSell
	}
	return SideBuy
}

func (e *Executor) reverseLeverage(sym string) int {
	cap := e.cfg.MaxLeverageCap
	if cap <= 0 {
		cap = 50
	}
	lev := e.client.MaxLeverage(sym)
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

func (e *Executor) reverseLiveMargin(sig *Signal) (float64, error) {
	bal, err := e.client.AvailableUSDTBalance()
	if err != nil {
		return 0, fmt.Errorf("fetch balance: %w", err)
	}
	if bal <= 0 {
		return 0, fmt.Errorf("available balance is zero")
	}
	if e.cfg.AllocationPercent <= 0 {
		return 0, fmt.Errorf("WHALE_ALLOCATION_PERCENT not set")
	}
	pct := e.cfg.AllocationPercent / 100
	maxPct := e.cfg.Risk.MaxPositionPercent / 100
	if maxPct > 0 && pct > maxPct {
		pct = maxPct
	}
	return bal * pct, nil
}

func (e *Executor) openReverseLive(sig *Signal, signalEntry float64) {
	if !e.cfg.ReverseLive || !e.client.Configured() {
		return
	}
	sym := sig.Symbol
	realSide := oppositeSide(sig.Side)

	lev := e.reverseLeverage(sym)
	if err := e.client.SetLeverage(sym, lev); err != nil {
		log.Printf("[whale] reverse set leverage %s %dx: %v", sym, lev, err)
	}
	margin, err := e.reverseLiveMargin(sig)
	if err != nil {
		log.Printf("[whale] reverse skip %s: %v", sym, err)
		return
	}
	notional := margin * float64(lev)
	if notional < 5 {
		log.Printf("[whale] reverse skip %s: notional %.2f < 5 USDT", sym, notional)
		return
	}

	start := time.Now()
	resp, err := e.client.MarketOrder(sym, string(realSide), notional)
	if err != nil {
		log.Printf("[whale] reverse OPEN failed %s %s: %v (%s)", realSide, sym, err, time.Since(start))
		return
	}
	entry, qty := parseFill(resp)
	if entry <= 0 || qty <= 0 {
		log.Printf("[whale] reverse OPEN bad fill %s %s", realSide, sym)
		return
	}

	rp := &reversePosition{
		Symbol: sym, SignalSide: sig.Side, RealSide: realSide,
		SignalEntry: signalEntry, EntryPrice: entry, Qty: qty,
		MarginUSDT: margin, Leverage: lev, OpenedAt: time.Now(),
	}
	e.mu.Lock()
	e.reverseLive[sym] = rp
	e.mu.Unlock()

	log.Printf("[whale] reverse OPEN %s %s signal=%s entry=%.6f live=%.6f qty=%.8f margin=%.2f lev=%dx (%s)",
		realSide, sym, sig.Side, signalEntry, entry, qty, margin, lev, time.Since(start))

	if e.journal != nil {
		e.journal.LogLiveEntry(sig, signalEntry, entry, margin, lev)
	}
}

func (e *Executor) reduceReverseLive(sym string, fraction float64) {
	if fraction <= 0 || fraction >= 1 {
		return
	}
	e.mu.Lock()
	rp, ok := e.reverseLive[sym]
	if !ok || rp.Partial {
		e.mu.Unlock()
		return
	}
	closeQty := rp.Qty * fraction
	if closeQty <= 0 {
		e.mu.Unlock()
		return
	}
	rp.Partial = true
	rp.Qty -= closeQty
	e.mu.Unlock()

	closeSide := string(oppositeSide(rp.RealSide))
	_, err := e.client.MarketOrderQty(sym, closeSide, closeQty)
	if err != nil {
		log.Printf("[whale] reverse PARTIAL close %s: %v", sym, err)
	}
}

func (e *Executor) closeReverseLive(sym string, signalExit float64, reason string, at time.Time) {
	if !e.cfg.ReverseLive {
		return
	}
	e.mu.Lock()
	rp, ok := e.reverseLive[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	delete(e.reverseLive, sym)
	e.mu.Unlock()

	closeSide := string(oppositeSide(rp.RealSide))
	var liveExit float64
	if rp.Qty > 0 {
		resp, err := e.client.MarketOrderQty(sym, closeSide, rp.Qty)
		if err != nil {
			log.Printf("[whale] reverse CLOSE failed %s %s: %v", closeSide, sym, err)
			liveExit, _ = e.client.MarkPrice(sym)
		} else {
			liveExit, _ = parseFill(resp)
		}
	}
	if liveExit <= 0 {
		liveExit = signalExit
	}

	simPos := &simPosition{
		Symbol: sym, Side: rp.RealSide, EntryPrice: rp.EntryPrice,
		MarginUSDT: rp.MarginUSDT, Leverage: rp.Leverage, OpenedAt: rp.OpenedAt,
		Partial: rp.Partial,
	}
	pnl := closeSimPnL(e.cfg.Risk, simPos, liveExit)
	ch := priceChangePct(rp.RealSide, rp.EntryPrice, liveExit)

	log.Printf("[whale] reverse EXIT %s %s reason=%s signal_exit=%.6f live_exit=%.6f pnl=%+.2f USDT (%+.2f%%) hold=%s",
		rp.RealSide, sym, reason, signalExit, liveExit, pnl, ch, at.Sub(rp.OpenedAt).Round(time.Second))

	if e.journal != nil {
		e.journal.LogLiveExit(rp, signalExit, liveExit, at, reason, pnl, ch)
	}
}
