package whale

import (
	"log"
	"strings"
	"time"
)

// Shadow mode: send real MARKET orders sized to fail (insufficient margin), record
// mark price at reject time for entry/exit logs — no position on exchange.

func (e *Executor) shadowSizing() (margin float64, lev int) {
	margin = e.cfg.ShadowMarginUSDT
	if margin <= 0 {
		margin = 100
	}
	lev = e.cfg.ShadowLeverage
	if lev <= 0 {
		lev = e.cfg.Leverage
	}
	if lev <= 0 {
		lev = 10
	}
	return margin, lev
}

// probePriceAtOrder is the best price snapshot right after a failed (or filled) order attempt.
func (e *Executor) probePriceAtOrder(sym string, fallback float64) float64 {
	mp, err := e.client.MarkPrice(sym)
	if err == nil && mp > 0 {
		return mp
	}
	return fallback
}

func slipBpsTrade(tradeSide Side, signalPx, fillPx float64) float64 {
	if signalPx <= 0 || fillPx <= 0 {
		return 0
	}
	bps := (fillPx - signalPx) / signalPx * 10000
	if tradeSide == SideSell {
		bps = -bps
	}
	return bps
}

func (e *Executor) openShadowLive(sig *Signal, signalEntry float64) {
	if !e.cfg.ShadowLive {
		return
	}
	if !e.client.Configured() {
		log.Printf("[whale] shadow skip %s: API keys required for probe orders", sig.Symbol)
		return
	}
	sym := sig.Symbol
	if !e.dryStillOpen(sym) {
		log.Printf("[whale] shadow skip %s: sim already closed", sym)
		return
	}

	tradeSide := e.cfg.TradeSide(sig.Side)
	margin, lev := e.shadowSizing()
	notional := margin * float64(lev)

	levEff := e.reverseLeverage(sym)
	_ = e.client.SetLeverage(sym, levEff)

	if !e.dryStillOpen(sym) {
		return
	}

	start := time.Now()
	resp, err := e.client.MarketOrder(sym, string(tradeSide), notional)
	execMs := time.Since(start)

	entry := e.probePriceAtOrder(sym, signalEntry)
	qty := 0.0
	if err == nil {
		entry, qty = parseFill(resp)
		if entry <= 0 {
			entry = e.probePriceAtOrder(sym, signalEntry)
		}
		log.Printf("[whale] shadow WARN %s: probe order FILLED (wanted fail) qty=%.8f — closing immediately", sym, qty)
		if qty > 0 {
			closeSide := string(oppositeSide(tradeSide))
			_, _ = e.client.MarketOrderQty(sym, closeSide, qty)
		}
		return
	}

	if !e.dryStillOpen(sym) {
		log.Printf("[whale] shadow skip %s: sim closed after probe", sym)
		return
	}
	if entry <= 0 {
		log.Printf("[whale] shadow skip %s: no probe entry price", sym)
		return
	}
	if qty <= 0 {
		qty = notional / entry
	}

	errShort := shortenErr(err)
	if !isMarginOrBalanceErr(err) {
		log.Printf("[whale] shadow note %s: probe OPEN failed with unexpected error (still logging probe price)", sym)
	}

	rp := &reversePosition{
		Symbol: sym, SignalSide: sig.Side, RealSide: tradeSide,
		SignalEntry: signalEntry, EntryPrice: entry, Qty: qty,
		MarginUSDT: margin, Leverage: lev, OpenedAt: time.Now(),
	}
	e.mu.Lock()
	e.shadowPos[sym] = rp
	e.mu.Unlock()

	slip := slipBpsTrade(tradeSide, signalEntry, entry)
	log.Printf("[whale] shadow PROBE OPEN %s %s (signal %s) notional=%.0f FAIL: %s | signal_px=%.6f probe_px=%.6f slip=%+.1f bps (%s)",
		tradeSide, sym, sig.Side, notional, errShort, signalEntry, entry, slip, execMs)

	if e.journal != nil {
		e.journal.LogShadowEntry(sig, tradeSide, signalEntry, entry, margin, lev, notional, errShort, execMs, slip)
	}
}

func (e *Executor) reduceShadowLive(sym string, fraction float64) {
	if fraction <= 0 || fraction >= 1 {
		return
	}
	e.mu.Lock()
	rp, ok := e.shadowPos[sym]
	if !ok || rp.Partial {
		e.mu.Unlock()
		return
	}
	rp.Partial = true
	e.mu.Unlock()
	log.Printf("[whale] shadow PARTIAL %s %s fraction=%.0f%% (probe only)", rp.RealSide, sym, fraction*100)
}

func (e *Executor) closeShadowLive(sym string, signalExit float64, reason string, at time.Time) {
	if !e.cfg.ShadowLive {
		return
	}
	e.mu.Lock()
	rp, ok := e.shadowPos[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	delete(e.shadowPos, sym)
	e.mu.Unlock()

	closeSide := string(oppositeSide(rp.RealSide))
	start := time.Now()
	resp, err := e.client.MarketOrderQty(sym, closeSide, rp.Qty)
	execMs := time.Since(start)

	exit := e.probePriceAtOrder(sym, signalExit)
	if err == nil {
		if px, _ := parseFill(resp); px > 0 {
			exit = px
		}
		log.Printf("[whale] shadow WARN %s: probe EXIT filled (wanted fail) — using fill price", sym)
	}

	errShort := shortenErr(err)
	if err != nil && !isMarginOrBalanceErr(err) {
		log.Printf("[whale] shadow note %s: probe EXIT failed with unexpected error", sym)
	}

	simPos := &simPosition{
		Symbol: sym, Side: rp.RealSide, EntryPrice: rp.EntryPrice,
		MarginUSDT: rp.MarginUSDT, Leverage: rp.Leverage, OpenedAt: rp.OpenedAt,
		Partial: rp.Partial,
	}
	pnl := closeSimPnL(e.cfg.RiskForExit(), simPos, exit)
	ch := priceChangePct(rp.RealSide, rp.EntryPrice, exit)
	exitSlip := slipBpsTrade(rp.RealSide, signalExit, exit)

	failNote := "FAIL"
	if err == nil {
		failNote = "FILLED"
	}
	log.Printf("[whale] shadow PROBE EXIT %s %s reason=%s %s: %s | signal_exit=%.6f probe_exit=%.6f slip=%+.1f bps pnl=%+.2f USDT (%+.2f%%) (%s)",
		rp.RealSide, sym, reason, failNote, errShort, signalExit, exit, exitSlip, pnl, ch, execMs)

	if e.journal != nil {
		e.journal.LogShadowExit(rp, signalExit, exit, at, reason, pnl, ch, errShort, execMs, exitSlip)
	}
}

func shortenErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// isMarginOrBalanceErr — expected when probe notional exceeds wallet margin.
func isMarginOrBalanceErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "margin") || strings.Contains(s, "balance") ||
		strings.Contains(s, "insufficient") || strings.Contains(s, "-2019") ||
		strings.Contains(s, "-2018") || strings.Contains(s, "-4164")
}

