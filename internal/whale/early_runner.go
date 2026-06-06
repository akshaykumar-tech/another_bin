package whale

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"crypto_announcements_go/internal/binance"
)

type EarlyRunner struct {
	cfg     Config
	client  *binance.FuturesClient
	ws      *FuturesWS
	exec    *Executor
	journal *TradeJournal
	focus   *FocusController

	monitors sync.Map
}

func NewEarlyRunner(cfg Config, client *binance.FuturesClient) (*EarlyRunner, error) {
	journal, err := NewTradeJournal(cfg.TradeLogPath)
	if err != nil {
		return nil, err
	}
	focus := NewFocusController(cfg.FocusCooldownSec)
	return &EarlyRunner{
		cfg:     cfg,
		client:  client,
		ws:      NewFuturesWS(cfg, focus),
		focus:   focus,
		exec:    NewExecutor(cfg, client, journal, focus),
		journal: journal,
	}, nil
}

func (r *EarlyRunner) TradeLogPath() string {
	if r == nil || r.journal == nil {
		return ""
	}
	return r.journal.Path()
}

func (r *EarlyRunner) Run(ctx context.Context) error {
	for _, sym := range r.cfg.Symbols {
		r.monitors.Store(sym, NewEarlyMonitor(r.cfg))
	}
	ew := r.cfg.Early
	log.Printf("[early] watch | rule=%s direction=%s lead=%dm hold=%dm scan=%dm | symbols=%d",
		ew.Rule, ew.Direction, ew.LeadMinutes, ew.HoldMinutes, ew.ScanStepMinutes, len(r.cfg.Symbols))
	log.Printf("[early] dry_run=%v dry_same=%v dry_rev_limit=%v live_trade=%v | same_margin=%s rev_margin=%s",
		r.cfg.DryRun, ew.DrySameEnabled, ew.DryReverseLimitEnabled, r.cfg.EarlyLiveTrade,
		earlyPathMarginNote(r.cfg, true), earlyPathMarginNote(r.cfg, false))

	nSym := len(r.cfg.Symbols)
	workers := nSym
	if workers < 4 {
		workers = 4
	}
	if workers > 32 {
		workers = 32
	}
	for i := 0; i < workers; i++ {
		go r.eventWorker(ctx)
	}
	return r.ws.Run(ctx)
}

func earlyPathMarginNote(c Config, same bool) string {
	if same {
		if c.EarlySameMarginUSDT > 0 {
			return fmt.Sprintf("%.2f USDT fixed", c.EarlySameMarginUSDT)
		}
		if c.EarlySameAllocationPercent > 0 {
			return fmt.Sprintf("%.0f%% balance lev=%d", c.EarlySameAllocationPercent, earlyPathLev(c, true))
		}
	} else {
		if c.EarlyReverseMarginUSDT > 0 {
			return fmt.Sprintf("%.2f USDT fixed", c.EarlyReverseMarginUSDT)
		}
		if c.EarlyReverseAllocationPercent > 0 {
			return fmt.Sprintf("%.0f%% balance lev=%d", c.EarlyReverseAllocationPercent, earlyPathLev(c, false))
		}
	}
	return fmt.Sprintf("%s lev=%d", earlyMarginNote(c), earlyPathLev(c, same))
}

func earlyPathLev(c Config, same bool) int {
	if same && c.EarlySameLeverage > 0 {
		return c.EarlySameLeverage
	}
	if !same && c.EarlyReverseLeverage > 0 {
		return c.EarlyReverseLeverage
	}
	if c.Leverage > 0 {
		return c.Leverage
	}
	return 1
}

func earlyMarginNote(c Config) string {
	if c.MarginUSDT > 0 {
		return fmt.Sprintf("%.2f USDT fixed", c.MarginUSDT)
	}
	if c.AllocationPercent > 0 {
		return fmt.Sprintf("%.0f%% of balance", c.AllocationPercent)
	}
	return fmt.Sprintf("%.0f USDT sim capital × risk", c.CapitalUSDT)
}

func (r *EarlyRunner) eventWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-r.ws.Events():
			if !ok {
				return
			}
			r.processEvent(ctx, ev)
		}
	}
}

func (r *EarlyRunner) processEvent(ctx context.Context, ev StreamEvent) {
	sym := ev.Symbol
	if r.focus != nil && !r.focus.AllowsEvent(sym) {
		return
	}
	if ev.Trade == nil {
		return
	}
	price, qty, buyerMaker := parseAggTrade(ev.Trade)
	if price <= 0 || qty <= 0 {
		return
	}
	at := tradeEventTime(ev.Recv, ev.Trade)

	raw, ok := r.monitors.Load(sym)
	if !ok {
		return
	}
	mon := raw.(*EarlyMonitor)

	if r.cfg.DryRun && r.cfg.Early.DryReverseLimitEnabled {
		r.exec.OnRevLimitTick(ctx, sym, price, at)
	}
	if r.cfg.DryRun && r.cfg.UsesTickDrySim() {
		if r.exec.HasDryPosition(sym) {
			r.exec.OnPriceTick(sym, price, at)
		}
		if r.exec.HasDryRevPosition(sym) {
			r.exec.OnRevPriceTick(sym, price, at)
		}
	}

	mon.OnTick(price, qty, buyerMaker, at)

	if sig := mon.TrySignal(sym, at); sig != nil {
		sig.RecvAt = ev.Recv
		tape := BuildEarlyTape(r.cfg.Burst, mon.copyTicks(), at)
		log.Printf("[early] SIGNAL %s %s rule=%s q60=$%.0f r60=%.2f%% t30=%d vol_accel=%.1fx px=%.6f",
			sig.Side, sym, r.cfg.Early.Rule,
			tape.Snap.Quiet60, tape.Snap.Range60, tape.Snap.Trades30, tape.VolAccel, sig.EntryPrice)

		if r.cfg.DryRun && r.cfg.Early.DrySameEnabled {
			if r.exec.CanEarlySameDry(sym, at) {
				r.exec.HandleSignal(ctx, sig)
			}
		}
		if r.cfg.DryRun && r.cfg.Early.DryReverseLimitEnabled {
			r.exec.TryStartReverseLimit(ctx, sig, tape.VolAccel)
		}
		if !r.cfg.DryRun && r.cfg.EarlyLiveTrade {
			r.exec.HandleSignal(ctx, sig)
		}
	}
}

func (m *EarlyMonitor) copyTicks() []binance.AggTrade {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]binance.AggTrade(nil), m.ticks...)
}

// LastTradeTime exposes cooldown state for early scans.
func (e *Executor) LastTradeTime(sym string) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastTrade[sym]
}

func formatEarlyStreams(cfg Config) string {
	return fmt.Sprintf("early aggTrade (%d symbols)", len(cfg.Symbols))
}
