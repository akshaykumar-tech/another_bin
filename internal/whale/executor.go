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
	cfg    Config
	client *binance.FuturesClient
	journal *TradeJournal

	mu        sync.Mutex
	openCount int
	lastTrade map[string]time.Time
	active    map[string]*position
	dryOpen   map[string]*simPosition
	dryPartial map[string]float64
}

type position struct {
	Symbol     string
	Side       Side
	EntryPrice float64
	Qty        float64
	MarginUSDT float64
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
	if !e.client.SymbolTradable(sym) {
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

	margin := e.marginForSignal(sig)
	side := string(sig.Side)

	if e.cfg.DryRun {
		entry, err := e.client.MarkPrice(sym)
		if err != nil || entry <= 0 {
			log.Printf("[whale] SIGNAL skip %s %s: mark price unavailable", side, sym)
			return
		}
		e.logSignalEntry(sig, entry, margin)
		if e.journal != nil {
			e.journal.LogEntry(sig, entry, margin)
		}
		sim := &simPosition{
			Symbol: sym, Side: sig.Side, EntryPrice: entry, MarginUSDT: margin,
			OpenedAt: time.Now(), MegaExit: sig.Mega || sig.Kind == SignalBurst, PeakPrice: entry,
		}
		e.mu.Lock()
		e.dryOpen[sym] = sim
		e.openCount++
		e.lastTrade[sym] = time.Now()
		e.mu.Unlock()
		go e.manageDryRunExit(ctx, sim)
		return
	}

	start := time.Now()
	resp, err := e.client.MarketOrder(sym, side, margin)
	execMs := time.Since(start)
	if err != nil {
		log.Printf("[whale] order failed %s %s: %v (%s)", side, sym, err, execMs)
		return
	}
	entry, qty := parseFill(resp)
	e.logSignalEntry(sig, entry, margin)
	if e.journal != nil {
		e.journal.LogEntry(sig, entry, margin)
	}

	pos := &position{
		Symbol: sym, Side: sig.Side, EntryPrice: entry, Qty: qty, MarginUSDT: margin,
		OpenedAt: time.Now(), MegaExit: sig.Mega || sig.Kind == SignalBurst, PeakPrice: entry,
	}
	e.mu.Lock()
	e.active[sym] = pos
	e.openCount++
	e.lastTrade[sym] = time.Now()
	e.mu.Unlock()

	go e.manageExit(ctx, pos)
}

func (e *Executor) logSignalEntry(sig *Signal, entry, margin float64) {
	switch sig.Kind {
	case SignalBookLead:
		log.Printf("[whale] SIGNAL %s %s book mode=%s imb=%.2fx flow=$%.0f move=%.2f%% entry=%.6f margin=%.2f",
			sig.Side, sig.Symbol, sig.BookMode, sig.ImbalanceRatio, sig.TradeFlowUSDT, sig.MovePct, entry, margin)
	case SignalBurst:
		log.Printf("[whale] SIGNAL %s %s BURST fast=%.2f%% 1s=%.2f%% vol=$%.0f entry=%.6f margin=%.2f",
			sig.Side, sig.Symbol, sig.FastMove, sig.MovePct, sig.SecVolume, entry, margin)
	default:
		log.Printf("[whale] SIGNAL %s %s flash mode=%s 1s=%.2f%% entry=%.6f margin=%.2f",
			sig.Side, sig.Symbol, sig.FlashMode, sig.MovePct, entry, margin)
	}
}

func (e *Executor) logExit(pos *simPosition, exitPrice float64, at time.Time, reason string, partialAlready float64) {
	pnl := closeSimPnL(e.cfg.Risk, pos, exitPrice)
	if partialAlready > 0 {
		pnl += partialAlready
	}
	ch := priceChangePct(pos.Side, pos.EntryPrice, exitPrice)
	log.Printf("[whale] EXIT %s %s reason=%s exit=%.6f pnl=%+.2f USDT (%+.2f%%) hold=%s",
		pos.Side, pos.Symbol, reason, exitPrice, pnl, ch, at.Sub(pos.OpenedAt).Round(time.Second))
}

func (e *Executor) marginForSignal(sig *Signal) float64 {
	capital := e.cfg.CapitalUSDT
	if e.cfg.UseLiveBalance {
		if bal, err := e.client.AvailableUSDTBalance(); err == nil && bal > 0 {
			capital = bal
		}
	}
	pct := e.cfg.Risk.NormalRiskPercent / 100
	if sig.Mega {
		pct = e.cfg.Risk.MegaRiskPercent / 100
	}
	maxPct := e.cfg.Risk.MaxPositionPercent / 100
	if pct > maxPct {
		pct = maxPct
	}
	return capital * pct
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

func (e *Executor) manageDryRunExit(ctx context.Context, pos *simPosition) {
	defer func() {
		e.mu.Lock()
		delete(e.dryOpen, pos.Symbol)
		delete(e.dryPartial, pos.Symbol)
		e.openCount--
		e.mu.Unlock()
	}()

	sym := pos.Symbol
	deadline := time.After(10 * time.Minute)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			e.finishDryExit(pos, sym, "ctx")
			return
		case <-deadline:
			e.finishDryExit(pos, sym, "timeout")
			return
		case <-tick.C:
			mp, err := e.client.MarkPrice(sym)
			if err != nil || mp <= 0 {
				continue
			}
			if e.dryExitStep(pos, mp) {
				return
			}
		}
	}
}

func (e *Executor) dryExitStep(pos *simPosition, price float64) bool {
	at := time.Now()
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
		e.finishDryExit(pos, pos.Symbol, reason)
		return true
	}
	return false
}

func (e *Executor) finishDryExit(pos *simPosition, sym, reason string) {
	mp, err := e.client.MarkPrice(sym)
	if err != nil || mp <= 0 {
		mp = pos.EntryPrice
	}
	at := time.Now()
	e.mu.Lock()
	part := e.dryPartial[sym]
	e.mu.Unlock()
	e.logExit(pos, mp, at, reason, part)
	if e.journal != nil {
		e.journal.LogExit(e.cfg.Risk, pos, mp, at, reason, part)
	}
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
	return &simPosition{
		Symbol: pos.Symbol, Side: pos.Side, EntryPrice: pos.EntryPrice,
		MarginUSDT: pos.MarginUSDT, OpenedAt: pos.OpenedAt, MegaExit: pos.MegaExit,
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
					partialUSDT += pos.MarginUSDT * partial * tp1Pct
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
				}
				if tp1Done && mp <= tp2Price {
					finish("tp2")
					return
				}
			}
		}
	}
}

func (e *Executor) closeAll(symbol, side string, qty float64) {
	if qty <= 0 || e.cfg.DryRun {
		return
	}
	_, _ = e.client.MarketOrderQty(symbol, side, qty)
}
