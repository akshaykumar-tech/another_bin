package whale

import (
	"sync"
	"time"

	"crypto_announcements_go/internal/binance"
)

const earlyTickRetain = 3 * time.Hour

type EarlyMonitor struct {
	cfg   Config
	mu    sync.Mutex
	ticks []binance.AggTrade
	lastScan time.Time
}

func NewEarlyMonitor(cfg Config) *EarlyMonitor {
	return &EarlyMonitor{cfg: cfg}
}

func (m *EarlyMonitor) OnTick(price, qty float64, buyerMaker bool, at time.Time) {
	if price <= 0 || qty <= 0 {
		return
	}
	m.mu.Lock()
	m.ticks = append(m.ticks, binance.AggTrade{
		Price: price, Quantity: qty, Time: at, BuyerIsMaker: buyerMaker,
	})
	cut := at.Add(-earlyTickRetain)
	i := 0
	for i < len(m.ticks) && m.ticks[i].Time.Before(cut) {
		i++
	}
	if i > 0 {
		m.ticks = append([]binance.AggTrade(nil), m.ticks[i:]...)
	}
	m.mu.Unlock()
}

func (m *EarlyMonitor) TrySignal(sym string, at time.Time) *Signal {
	ew := m.cfg.Early
	step := ew.scanStep()
	if step <= 0 {
		step = 5 * time.Minute
	}

	m.mu.Lock()
	if !m.lastScan.IsZero() && at.Sub(m.lastScan) < step {
		m.mu.Unlock()
		return nil
	}
	m.lastScan = at
	ticks := append([]binance.AggTrade(nil), m.ticks...)
	m.mu.Unlock()

	if len(ticks) < 50 {
		return nil
	}

	tape := BuildEarlyTape(m.cfg.Burst, ticks, at)
	if !ew.RuleFires(tape) {
		return nil
	}

	short, ok := EarlyDirection(ew.Direction, ticks, at, 0)
	if !ok {
		return nil
	}
	side := SideBuy
	if short {
		side = SideSell
	}
	px := earlyPriceAt(ticks, at)
	if px <= 0 {
		return nil
	}

	return &Signal{
		Symbol:     sym,
		Side:       side,
		Kind:       SignalEarly,
		MovePct:    tape.Snap.Range60,
		EntryPrice: px,
		RecvAt:     at,
	}
}
