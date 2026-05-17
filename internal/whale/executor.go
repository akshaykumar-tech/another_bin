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

	mu        sync.Mutex
	openCount int
	lastTrade map[string]time.Time
	active    map[string]*position
}

type position struct {
	Symbol     string
	Side       Side
	EntryPrice float64
	Qty        float64
	OpenedAt   time.Time
	MegaExit   bool
	PeakPrice  float64
	Partial    bool
}

func NewExecutor(cfg Config, client *binance.FuturesClient) *Executor {
	return &Executor{
		cfg:       cfg,
		client:    client,
		lastTrade: make(map[string]time.Time),
		active:    make(map[string]*position),
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
	if _, busy := e.active[sym]; busy {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	start := time.Now()
	margin := e.marginForSignal(sig)
	side := string(sig.Side)
	if e.cfg.DryRun {
		switch sig.Kind {
		case SignalBookLead:
			log.Printf("[whale] DRY_RUN %s %s book mode=%s imb=%.2fx flow=$%.0f move=%.2f%% margin=%.2f USDT latency=%s",
				side, sym, sig.BookMode, sig.ImbalanceRatio, sig.TradeFlowUSDT, sig.MovePct, margin, time.Since(sig.RecvAt))
		case SignalBurst:
			log.Printf("[whale] DRY_RUN %s %s BURST fast=%.2f%% 1s=%.2f%% vol=$%.0f margin=%.2f USDT mega_trail latency=%s",
				side, sym, sig.FastMove, sig.MovePct, sig.SecVolume, margin, time.Since(sig.RecvAt))
		default:
			log.Printf("[whale] DRY_RUN %s %s 1s=%.2f%% mode=%s margin=%.2f USDT latency=%s",
				side, sym, sig.MovePct, sig.FlashMode, margin, time.Since(sig.RecvAt))
		}
		e.markCooldown(sym)
		return
	}

	resp, err := e.client.MarketOrder(sym, side, margin)
	execMs := time.Since(start)
	if err != nil {
		log.Printf("[whale] order failed %s %s: %v (%s)", side, sym, err, execMs)
		return
	}
	entry, qty := parseFill(resp)
	log.Printf("[whale] ENTER %s %s margin=%.2f entry=%.6f qty=%.6f order_ms=%s total_ms=%s",
		side, sym, margin, entry, qty, execMs, time.Since(sig.RecvAt))

	pos := &position{
		Symbol: sym, Side: sig.Side, EntryPrice: entry, Qty: qty, OpenedAt: time.Now(),
		MegaExit: sig.Mega || sig.Kind == SignalBurst, PeakPrice: entry,
	}
	e.mu.Lock()
	e.active[sym] = pos
	e.openCount++
	e.lastTrade[sym] = time.Now()
	e.mu.Unlock()

	go e.manageExit(ctx, pos)
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

func (e *Executor) markCooldown(sym string) {
	e.mu.Lock()
	e.lastTrade[sym] = time.Now()
	e.mu.Unlock()
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

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(10 * time.Minute)

	closeRemaining := func(reason string) {
		if remaining > 0 {
			e.closeAll(pos.Symbol, closeSide, remaining)
			log.Printf("[whale] mega exit %s %s reason=%s", pos.Symbol, pos.Side, reason)
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
			sim := &simPosition{
				Symbol: pos.Symbol, Side: pos.Side, EntryPrice: pos.EntryPrice,
				MarginUSDT: 1, OpenedAt: pos.OpenedAt, MegaExit: true,
				PeakPrice: pos.PeakPrice, Partial: partialDone,
			}
			closed, reason, partialPnL := megaExitStep(r, sim, mp, time.Now())
			pos.PeakPrice = sim.PeakPrice
			if partialPnL > 0 && !partialDone {
				half := remaining * partialFrac
				_, _ = e.client.MarketOrderQty(pos.Symbol, closeSide, half)
				remaining -= half
				partialDone = true
				pos.Partial = true
				log.Printf("[whale] mega TP1 partial %s at %.6f", pos.Symbol, mp)
			}
			if closed {
				closeRemaining(reason)
				return
			}
		}
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

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(15 * time.Minute)

	closeRemaining := func() {
		if remaining > 0 {
			e.closeAll(pos.Symbol, closeSide, remaining)
		}
	}

	for {
		select {
		case <-ctx.Done():
			closeRemaining()
			return
		case <-deadline:
			closeRemaining()
			return
		case <-tick.C:
			mp, err := e.client.MarkPrice(pos.Symbol)
			if err != nil || mp <= 0 {
				continue
			}
			if pos.Side == SideBuy {
				if mp <= slPrice {
					closeRemaining()
					return
				}
				if !tp1Done && mp >= tp1Price {
					if halfQty > 0 {
						_, _ = e.client.MarketOrderQty(pos.Symbol, closeSide, halfQty)
						remaining -= halfQty
					}
					tp1Done = true
				}
				if tp1Done && mp >= tp2Price {
					closeRemaining()
					return
				}
			} else {
				if mp >= slPrice {
					closeRemaining()
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
					closeRemaining()
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
