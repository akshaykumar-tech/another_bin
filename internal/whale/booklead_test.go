package whale

import (
	"testing"
	"time"
)

func TestBookLeadLongBeforeBigMove(t *testing.T) {
	cfg := DefaultConfig().BookLead
	cfg.WindowMs = 1000
	cfg.MinImbalanceRatio = 2.0
	cfg.MaxThinSideUSDT = 50_000
	cfg.MinTradeNotionalUSDT = 5000
	cfg.MinBookSideUSDT = 1000
	cfg.SignalCooldownMs = 0

	d := NewBookLeadDetector(cfg)
	now := time.Now()

	bids := [][]string{
		{"1.00", "50000"},
		{"0.99", "40000"},
		{"0.98", "30000"},
	}
	asks := [][]string{
		{"1.01", "500"},
		{"1.02", "400"},
		{"1.03", "300"},
	}

	for i := 0; i < 15; i++ {
		d.OnAggTrade(1.005, 2000, false, now.Add(time.Duration(i)*60*time.Millisecond))
	}
	sig := d.OnDepth(bids, asks, now.Add(900*time.Millisecond))
	if sig == nil {
		t.Fatal("expected long book-lead signal")
	}
	if sig.Side != SideBuy {
		t.Fatalf("side=%s", sig.Side)
	}
	if sig.Kind != SignalBookLead {
		t.Fatalf("kind=%s", sig.Kind)
	}
}

func TestBookLeadSkipsWhenMoveCapEnabled(t *testing.T) {
	cfg := DefaultConfig().BookLead
	cfg.MaxEntryMovePct = 0.5 // optional cap when explicitly set
	cfg.SignalCooldownMs = 0
	d := NewBookLeadDetector(cfg)
	now := time.Now()

	bids := [][]string{{"1.00", "50000"}}
	asks := [][]string{{"1.01", "500"}}

	for i := 0; i < 10; i++ {
		p := 1.0 + float64(i)*0.02
		d.OnAggTrade(p, 3000, false, now.Add(time.Duration(i)*80*time.Millisecond))
	}
	sig := d.OnDepth(bids, asks, now.Add(800*time.Millisecond))
	if sig != nil {
		t.Fatalf("expected no signal when move cap exceeded, got %+v", sig)
	}
}

func TestBookLeadAllowsViolentMoveWithCapOff(t *testing.T) {
	cfg := DefaultConfig().BookLead
	cfg.MaxEntryMovePct = 0 // off — ~18% move in window must not block
	cfg.SignalCooldownMs = 0
	cfg.MinTradeNotionalUSDT = 5000
	cfg.MinImbalanceRatio = 2.0
	cfg.MaxThinSideUSDT = 50_000
	cfg.MinBookSideUSDT = 1000
	d := NewBookLeadDetector(cfg)
	now := time.Now()

	bids := [][]string{{"1.00", "50000"}, {"0.99", "40000"}}
	asks := [][]string{{"1.01", "600"}, {"1.02", "500"}}

	for i := 0; i < 10; i++ {
		p := 1.0 + float64(i)*0.02
		d.OnAggTrade(p, 3000, false, now.Add(time.Duration(i)*80*time.Millisecond))
	}
	sig := d.OnDepth(bids, asks, now.Add(800*time.Millisecond))
	if sig == nil {
		t.Fatal("expected signal during violent move when max_entry_move_pct is off")
	}
}
