package whale

import (
	"log"
	"time"

	"crypto_announcements_go/internal/binance"
)

// RunBurstBacktest replays aggTrades with burst detector and mega trailing exits.
func RunBurstBacktest(cfg Config, client *binance.FuturesClient, symbol string, trades []binance.AggTrade, verbose bool) BacktestSummary {
	st := newBurstReplay(cfg, client, verbose, time.Time{}, time.Time{})
	st.summary.Symbols = 1
	for _, tr := range trades {
		st.onTrade(symbol, tr)
	}
	if len(trades) > 0 {
		last := trades[len(trades)-1]
		if pos, ok := st.open[symbol]; ok {
			reason := "timeout"
			if pe := st.pendingExit[symbol]; pe != nil {
				reason = pe.Reason
				delete(st.pendingExit, symbol)
			}
			st.closePosition(symbol, pos, last.Price, last.Time, reason)
		}
	}
	return st.summary
}

type burstReplayState struct {
	replayState
	burst    *BurstDetector
	pumpFrom time.Time
	pumpTo   time.Time
	pump     PumpCaptureResult
}

func newBurstReplay(cfg Config, client *binance.FuturesClient, verbose bool, pumpFrom, pumpTo time.Time) *burstReplayState {
	return &burstReplayState{
		replayState: replayState{
			cfg:       cfg,
			client:    client,
			open:      make(map[string]*simPosition),
			pending:     make(map[string]*pendingBurstEntry),
			pendingExit: make(map[string]*pendingExit),
			lastTrade: make(map[string]time.Time),
			verbose:   verbose,
		},
		burst:    NewBurstDetector(cfg.Burst),
		pumpFrom: pumpFrom,
		pumpTo:   pumpTo,
	}
}

func (st *burstReplayState) onTrade(sym string, tr binance.AggTrade) {
	st.summary.TradesLoaded++
	st.tryFillPendingExit(sym, tr.Price, tr.Time)
	st.tryFillPendingEntry(sym, tr)
	st.markToMarket(sym, tr.Price, tr.Time)

	sig := st.burst.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
	if sig == nil {
		return
	}
	tradeSide := st.cfg.TradeSide(sig.Side)
	st.summary.Signals++
	if st.openCount >= st.cfg.MaxOpenPositions {
		st.summary.SkippedMaxPos++
		return
	}
	if t, ok := st.lastTrade[sym]; ok && tr.Time.Sub(t) < st.cfg.Cooldown() {
		st.summary.SkippedCooldown++
		return
	}
	if _, busy := st.open[sym]; busy {
		st.summary.SkippedCooldown++
		return
	}
	if pend := st.pending[sym]; pend != nil {
		return
	}
	margin := st.marginFor(sig)
	if tr.Price <= 0 {
		return
	}
	if st.verbose {
		log.Printf("[backtest] BURST %s %s fast=%.2f%% 1s=%.2f%% vol=$%.0f @ %s",
			sig.Side, sym, sig.FastMove, sig.MovePct, sig.SecVolume, tr.Time.Format("15:04:05.000"))
	}
	delay := time.Duration(st.cfg.BacktestEntryDelayMs) * time.Millisecond
	if delay <= 0 {
		st.openBurstPosition(sym, tradeSide, margin, tr.Time, tr.Price)
		return
	}
		st.pending[sym] = &pendingBurstEntry{
		Side: tradeSide, MarginUSDT: margin, MegaExit: true,
		SignalAt: tr.Time, EnterAfter: tr.Time.Add(delay),
	}
}

func (st *burstReplayState) tryFillPendingEntry(sym string, tr binance.AggTrade) {
	p, ok := st.pending[sym]
	if !ok || p == nil || tr.Time.Before(p.EnterAfter) || tr.Price <= 0 {
		return
	}
	delete(st.pending, sym)
	px := tr.Price
	if bps := st.cfg.BacktestEntrySlippageBps; bps > 0 {
		px = applySlippage(px, p.Side, bps, true)
	} else if bps := st.cfg.DryEntrySlippageBps; bps > 0 {
		px = applySlippage(px, p.Side, bps, true)
	}
	st.openBurstPosition(sym, p.Side, p.MarginUSDT, tr.Time, px)
}

func (st *burstReplayState) openBurstPosition(sym string, side Side, margin float64, at time.Time, entry float64) {
	if entry <= 0 {
		return
	}
	st.open[sym] = &simPosition{
		Symbol: sym, Side: side, EntryPrice: entry, MarginUSDT: margin,
		Leverage: st.simLeverage(), OpenedAt: at, MegaExit: true, PeakPrice: entry,
	}
	st.openCount++
	st.lastTrade[sym] = at
	st.summary.Entries++
	if !st.pumpFrom.IsZero() && !st.pumpTo.IsZero() &&
		!at.Before(st.pumpFrom) && !at.After(st.pumpTo) && !st.pump.Found {
		st.pump.Found = true
		st.pump.EntryTime = at
		st.pump.EntryPrice = entry
		st.pump.Signals = st.summary.Signals
	}
}

func (st *burstReplayState) closePosition(sym string, pos *simPosition, price float64, at time.Time, reason string) {
	if st.pump.Found && pos.EntryPrice > 0 && price > 0 {
		var ch, peak float64
		if pos.Side == SideBuy {
			ch = (price - pos.EntryPrice) / pos.EntryPrice * 100
			peak = (pos.PeakPrice - pos.EntryPrice) / pos.EntryPrice * 100
		} else {
			ch = (pos.EntryPrice - price) / pos.EntryPrice * 100
			peak = (pos.EntryPrice - pos.PeakPrice) / pos.EntryPrice * 100
		}
		if pos.OpenedAt.Equal(st.pump.EntryTime) || st.pump.ExitReason == "" {
			st.pump.CapturePct = ch
			st.pump.PeakPct = peak
			st.pump.ExitPrice = price
			st.pump.ExitTime = at
			st.pump.ExitReason = reason
		}
	}
	st.replayState.closePosition(sym, pos, price, at, reason)
}

func (st *burstReplayState) markToMarket(sym string, price float64, at time.Time) {
	st.replayState.markToMarket(sym, price, at)
	pos, ok := st.open[sym]
	if !ok || !st.pump.Found || !pos.OpenedAt.Equal(st.pump.EntryTime) {
		return
	}
	if pos.Side == SideBuy {
		st.pump.PeakPct = (pos.PeakPrice - pos.EntryPrice) / pos.EntryPrice * 100
		st.pump.CapturePct = (price - pos.EntryPrice) / pos.EntryPrice * 100
	} else {
		st.pump.PeakPct = (pos.EntryPrice - pos.PeakPrice) / pos.EntryPrice * 100
		st.pump.CapturePct = (pos.EntryPrice - price) / pos.EntryPrice * 100
	}
	st.pump.ExitPrice = price
	st.pump.ExitTime = at
}

// ReplayBurstPump replays burst+mega exit around a pump window and returns capture stats.
func ReplayBurstPump(cfg Config, symbol string, trades []binance.AggTrade, warmupFrom, pumpFrom, pumpTo, end time.Time) PumpCaptureResult {
	st := newBurstReplay(cfg, nil, false, pumpFrom, pumpTo)
	for _, tr := range trades {
		if tr.Time.Before(warmupFrom) {
			continue
		}
		if tr.Time.After(end) {
			break
		}
		st.onTrade(symbol, tr)
	}
	for _, tr := range trades {
		if tr.Time.Before(pumpTo) {
			continue
		}
		if tr.Time.After(end) {
			break
		}
		st.markToMarket(symbol, tr.Price, tr.Time)
	}
	if pos, ok := st.open[symbol]; ok {
		last := trades[len(trades)-1]
		for i := len(trades) - 1; i >= 0; i-- {
			if !trades[i].Time.After(end) {
				last = trades[i]
				break
			}
		}
		st.closePosition(symbol, pos, last.Price, last.Time, "timeout")
	}
	st.pump.Signals = st.summary.Signals
	return st.pump
}

// PumpCaptureResult is burst entry/exit stats during a pump window.
type PumpCaptureResult struct {
	Found      bool
	EntryTime  time.Time
	ExitTime   time.Time
	EntryPrice float64
	ExitPrice  float64
	ExitReason string
	CapturePct float64
	PeakPct    float64
	Signals    int
}
