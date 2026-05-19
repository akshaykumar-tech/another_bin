package whale

import (
	"log"
	"time"

	"crypto_announcements_go/internal/binance"
)

const backtestHoldTimeout = 10 * time.Minute

type simPosition struct {
	Symbol      string
	Side        Side
	EntryPrice  float64
	MarginUSDT  float64
	Leverage    int // 0/1 = no mult; else PnL uses margin*leverage
	OpenedAt    time.Time
	Partial     bool
	MegaExit    bool
	PeakPrice   float64
	LastPrice   float64 // last mark/tick for dry timeout exit
}

func (p *simPosition) NotionalUSDT() float64 {
	lev := p.Leverage
	if lev <= 0 {
		lev = 1
	}
	return p.MarginUSDT * float64(lev)
}

type replayState struct {
	cfg    Config
	client *binance.FuturesClient

	flash map[string]*FlashDetector

	open      map[string]*simPosition
	openCount int
	lastTrade map[string]time.Time

	summary BacktestSummary
	verbose bool
}

func newReplayState(cfg Config, client *binance.FuturesClient, verbose bool) *replayState {
	st := &replayState{
		cfg:       cfg,
		client:    client,
		flash:     make(map[string]*FlashDetector),
		open:      make(map[string]*simPosition),
		lastTrade: make(map[string]time.Time),
		verbose:   verbose,
	}
	for _, sym := range cfg.Symbols {
		st.flash[sym] = NewFlashDetector(cfg.Flash)
	}
	st.summary.Symbols = len(cfg.Symbols)
	return st
}

func (st *replayState) replay(ds *marketDataset) BacktestSummary {
	for _, tr := range ds.trades {
		st.summary.TradesLoaded++
		st.onTrade(tr)
	}
	now := time.Time{}
	if len(ds.trades) > 0 {
		now = ds.trades[len(ds.trades)-1].Time
	}
	for sym, pos := range st.open {
		st.closePosition(sym, pos, lastPriceForSymbol(ds, sym), now, "timeout")
	}
	return st.summary
}

func lastPriceForSymbol(ds *marketDataset, sym string) float64 {
	idxs := ds.bySym[sym]
	if len(idxs) == 0 {
		return 0
	}
	return ds.trades[idxs[len(idxs)-1]].Price
}

func (st *replayState) onTrade(tr replayTrade) {
	st.markToMarket(tr.Symbol, tr.Price, tr.Time)

	det := st.flash[tr.Symbol]
	if det == nil {
		return
	}
	sig := det.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
	if sig == nil {
		return
	}
	sig.Symbol = tr.Symbol
	st.dispatchSignal(sig, tr.Price)
}

func (st *replayState) dispatchSignal(sig *Signal, entry float64) {
	st.summary.Signals++
	if st.client != nil && !st.client.SymbolTradable(sig.Symbol) {
		st.summary.SkippedNotTradable++
		return
	}
	if st.openCount >= st.cfg.MaxOpenPositions {
		st.summary.SkippedMaxPos++
		return
	}
	if t, ok := st.lastTrade[sig.Symbol]; ok && sig.RecvAt.Sub(t) < st.cfg.Cooldown() {
		st.summary.SkippedCooldown++
		return
	}
	if _, busy := st.open[sig.Symbol]; busy {
		st.summary.SkippedCooldown++
		return
	}

	margin := st.marginFor(sig)
	if entry <= 0 {
		return
	}
	if st.verbose {
		log.Printf("[backtest] FLASH %s %s mode=%s 1s=%.2f%% 100ms=%.2f%%",
			sig.Side, sig.Symbol, sig.FlashMode, sig.MovePct, sig.FastMove)
		log.Printf("[backtest] DRY_RUN %s %s margin=%.2f entry=%.6f", sig.Side, sig.Symbol, margin, entry)
	}
	st.open[sig.Symbol] = &simPosition{
		Symbol: sig.Symbol, Side: sig.Side, EntryPrice: entry, MarginUSDT: margin,
		OpenedAt: sig.RecvAt, MegaExit: sig.Mega || sig.Kind == SignalBurst,
		PeakPrice: entry,
	}
	st.openCount++
	st.lastTrade[sig.Symbol] = sig.RecvAt
	st.summary.Entries++
}

func (st *replayState) marginFor(sig *Signal) float64 {
	capital := st.cfg.CapitalUSDT
	pct := st.cfg.Risk.NormalRiskPercent / 100
	if sig.Mega {
		pct = st.cfg.Risk.MegaRiskPercent / 100
	}
	maxPct := st.cfg.Risk.MaxPositionPercent / 100
	if pct > maxPct {
		pct = maxPct
	}
	return capital * pct
}

func (st *replayState) markToMarket(sym string, price float64, at time.Time) {
	pos, ok := st.open[sym]
	if !ok || price <= 0 {
		return
	}

	if at.Sub(pos.OpenedAt) >= backtestHoldTimeout {
		st.closePosition(sym, pos, price, at, "timeout")
		return
	}

	r := st.cfg.Risk
	var closed bool
	var reason string
	var partial float64
	if pos.MegaExit {
		closed, reason, partial = megaExitStep(r, pos, price, at)
	} else {
		closed, reason, partial = standardExitStep(r, pos, price)
	}
	if partial > 0 {
		st.summary.ExitsTP1++
		st.summary.TotalPnLUSDT += partial
	}
	if closed {
		st.closePosition(sym, pos, price, at, reason)
	}
}

func (st *replayState) closePosition(sym string, pos *simPosition, price float64, at time.Time, reason string) {
	if price <= 0 {
		price = pos.EntryPrice
	}
	pnl := closeSimPnL(st.cfg.Risk, pos, price)
	st.summary.TotalPnLUSDT += pnl
	switch reason {
	case "sl":
		st.summary.ExitsSL++
	case "tp2", "mega_tp":
		st.summary.ExitsTP2++
	case "trail":
		st.summary.ExitsTP2++
	default:
		st.summary.ExitsTimeout++
	}
	if st.verbose {
		ch := 0.0
		if pos.EntryPrice > 0 {
			if pos.Side == SideBuy {
				ch = (price - pos.EntryPrice) / pos.EntryPrice * 100
			} else {
				ch = (pos.EntryPrice - price) / pos.EntryPrice * 100
			}
		}
		log.Printf("[backtest] EXIT %s %s %s pnl=%.2f%% usdt=%.2f", pos.Side, sym, reason, ch, pnl)
	}
	delete(st.open, sym)
	st.openCount--
}
