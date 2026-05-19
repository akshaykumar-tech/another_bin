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

	mu         sync.Mutex
	openCount  int
	lastTrade  map[string]time.Time
	active     map[string]*position
	dryOpen    map[string]*simPosition
	dryPartial map[string]float64
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

func NewExecutor(cfg Config, client *binance.FuturesClient, journal *TradeJournal) *Executor {
	return &Executor{
		cfg:        cfg,
		client:     client,
		journal:    journal,
		lastTrade:  make(map[string]time.Time),
		active:     make(map[string]*position),
		dryOpen:    make(map[string]*simPosition),
		dryPartial: make(map[string]float64),
	}
}

func (e *Executor) HandleSignal(ctx context.Context, sig *Signal) {
	if sig == nil {
		return
	}
	sym := sig.Symbol
	if !e.cfg.DryRun && !e.client.SymbolTradable(sym) {
		return
	}

	e.mu.Lock()
	if e.openCount >= e.cfg.MaxOpenPositions {
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
	} else if _, busy := e.active[sym]; busy {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	margin, lev := e.marginAndLeverage(sig)
	side := string(sig.Side)
	simMode := e.cfg.DrySimMode
	if simMode == "" {
		simMode = "tick"
	}

	if e.cfg.DryRun {
		entry := e.simEntryPrice(sig)
		if entry <= 0 {
			log.Printf("[whale] SIGNAL skip %s %s: no entry price", side, sym)
			return
		}
		e.logSignalEntry(sig, entry, margin, lev)
		if e.journal != nil {
			e.journal.LogEntry(sig, entry, margin, lev, simMode)
		}
		sim := &simPosition{
			Symbol: sym, Side: sig.Side, EntryPrice: entry, MarginUSDT: margin, Leverage: lev,
			OpenedAt: time.Now(), MegaExit: sig.Mega || sig.Kind == SignalBurst,
			PeakPrice: entry, LastPrice: entry,
		}
		e.mu.Lock()
		e.dryOpen[sym] = sim
		e.openCount++
		e.lastTrade[sym] = time.Now()
		e.mu.Unlock()
		if e.cfg.UsesTickDrySim() {
			go e.dryRunTimeout(ctx, sym)
		} else {
			go e.manageDryRunMarkPoll(ctx, sim)
		}
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
	e.logSignalEntry(sig, entry, margin, effLev)
	if e.journal != nil {
		e.journal.LogEntry(sig, entry, margin, effLev, "live")
	}

	pos := &position{
		Symbol: sym, Side: sig.Side, EntryPrice: entry, Qty: qty, MarginUSDT: margin, Leverage: effLev,
		OpenedAt: time.Now(), MegaExit: sig.Mega || sig.Kind == SignalBurst, PeakPrice: entry,
	}
	e.mu.Lock()
	e.active[sym] = pos
	e.openCount++
	e.lastTrade[sym] = time.Now()
	e.mu.Unlock()

	go e.manageExit(ctx, pos)
}

// OnPriceTick updates open dry positions from live aggTrade (tick sim mode).
func (e *Executor) OnPriceTick(sym string, price float64, at time.Time) {
	if !e.cfg.DryRun || !e.cfg.UsesTickDrySim() || price <= 0 {
		return
	}
	e.mu.Lock()
	pos, ok := e.dryOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	px := applySlippage(price, pos.Side, e.cfg.DryExitSlippageBps, false)
	pos.LastPrice = px
	if e.dryExitStep(pos, px, at) {
		e.mu.Lock()
		delete(e.dryOpen, sym)
		delete(e.dryPartial, sym)
		e.openCount--
		e.mu.Unlock()
	}
}

func (e *Executor) simEntryPrice(sig *Signal) float64 {
	entry := sig.EntryPrice
	if entry <= 0 {
		var err error
		entry, err = e.client.MarkPrice(sig.Symbol)
		if err != nil || entry <= 0 {
			return 0
		}
	}
	return applySlippage(entry, sig.Side, e.cfg.DryEntrySlippageBps, true)
}

func (e *Executor) marginAndLeverage(sig *Signal) (margin float64, leverage int) {
	capital := e.cfg.CapitalUSDT
	if e.cfg.UseLiveBalance {
		if bal, err := e.client.AvailableUSDTBalance(); err == nil && bal > 0 {
			capital = bal
		}
	}
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
	lev := e.cfg.Leverage
	if lev <= 0 {
		lev = 1
	}
	return margin, lev
}

func (e *Executor) logSignalEntry(sig *Signal, entry, margin float64, leverage int) {
	switch sig.Kind {
	case SignalBookLead:
		log.Printf("[whale] SIGNAL %s %s book mode=%s imb=%.2fx flow=$%.0f move=%.2f%% entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, sig.BookMode, sig.ImbalanceRatio, sig.TradeFlowUSDT, sig.MovePct, entry, margin, leverage)
	case SignalBurst:
		log.Printf("[whale] SIGNAL %s %s BURST fast=%.2f%% 1s=%.2f%% vol=$%.0f entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, sig.FastMove, sig.MovePct, sig.SecVolume, entry, margin, leverage)
	default:
		log.Printf("[whale] SIGNAL %s %s flash mode=%s 1s=%.2f%% entry=%.6f margin=%.2f lev=%dx",
			sig.Side, sig.Symbol, sig.FlashMode, sig.MovePct, entry, margin, leverage)
	}
}

func (e *Executor) logExit(pos *simPosition, exitPrice float64, at time.Time, reason string, partialAlready float64) {
	pnl := closeSimPnL(e.cfg.Risk, pos, exitPrice)
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

func (e *Executor) dryRunTimeout(ctx context.Context, sym string) {
	select {
	case <-ctx.Done():
		e.closeDry(sym, "ctx")
	case <-time.After(10 * time.Minute):
		e.closeDry(sym, "timeout")
	}
}

func (e *Executor) closeDry(sym, reason string) {
	e.mu.Lock()
	pos, ok := e.dryOpen[sym]
	if !ok {
		e.mu.Unlock()
		return
	}
	delete(e.dryOpen, sym)
	part := e.dryPartial[sym]
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
		e.journal.LogExit(e.cfg.Risk, pos, exit, at, reason, part)
	}
}

func (e *Executor) manageDryRunMarkPoll(ctx context.Context, pos *simPosition) {
	sym := pos.Symbol
	deadline := time.After(10 * time.Minute)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			e.closeDry(sym, "ctx")
			return
		case <-deadline:
			e.closeDry(sym, "timeout")
			return
		case <-tick.C:
			mp, err := e.client.MarkPrice(sym)
			if err != nil || mp <= 0 {
				continue
			}
			pos.LastPrice = mp
			if e.dryExitStep(pos, mp, time.Now()) {
				e.mu.Lock()
				delete(e.dryOpen, sym)
				delete(e.dryPartial, sym)
				e.openCount--
				e.mu.Unlock()
				return
			}
		}
	}
}

func (e *Executor) dryExitStep(pos *simPosition, price float64, at time.Time) bool {
	r := e.cfg.Risk
	var closed bool
	var reason string
	var partial float64
	if pos.MegaExit {
		closed, reason, partial = megaExitStep(r, pos, price, at)
	} else {
		closed, reason, partial = standardExitStep(r, pos, price)
	}
	if partial > 0 && !pos.Partial {
		pos.Partial = true
		e.mu.Lock()
		e.dryPartial[pos.Symbol] += partial
		e.mu.Unlock()
		if e.journal != nil {
			e.journal.LogPartial(pos.Symbol, pos.Side, partial, at)
		}
	}
	if closed {
		part := 0.0
		e.mu.Lock()
		part = e.dryPartial[pos.Symbol]
		delete(e.dryPartial, pos.Symbol)
		e.mu.Unlock()
		e.logExit(pos, price, at, reason, part)
		if e.journal != nil {
			e.journal.LogExit(e.cfg.Risk, pos, price, at, reason, part)
		}
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
			closed, reason, partialPnL := megaExitStep(r, sim, mp, time.Now())
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
			e.journal.LogExit(e.cfg.Risk, sim, mp, time.Now(), reason, partialUSDT)
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
