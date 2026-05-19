package whale

import (
	"testing"
	"time"
)

func TestPreTradeMegaOnlyFlatProfile(t *testing.T) {
	cfg := BurstConfig{
		PreTradeEnabled:    true,
		PreTradeMegaOnly:   true,
		PreTradeWindowMs:   60_000,
		PreTradeShortWindowMs: 30_000,
	}
	d := NewBurstDetector(cfg)
	now := time.Date(2026, 5, 13, 8, 0, 6, 0, time.UTC) // 13:30 IST

	// Sparse dead tape: only 2 prints in the last 2s before burst (MLN 13:30 t30=2).
	for i := 58; i < 60; i++ {
		at := now.Add(-time.Duration(61-i) * time.Second)
		d.ticks = append(d.ticks, tradeTick{at: at, price: 100, qty: 0.01, buyAggressive: true})
	}
	if reason := d.explainPreTradeReject(now); reason != "" {
		t.Fatalf("flat mega tape should pass, got %q", reason)
	}
}

func TestPreTradeMegaOnlyRejectsChop(t *testing.T) {
	cfg := BurstConfig{
		PreTradeEnabled:  true,
		PreTradeMegaOnly: true,
		PreTradeWindowMs: 60_000,
		PreTradeShortWindowMs: 30_000,
	}
	d := NewBurstDetector(cfg)
	now := time.Date(2026, 5, 15, 8, 46, 42, 0, time.UTC)

	for i := 0; i < 40; i++ {
		at := now.Add(-time.Duration(40-i) * time.Second)
		qty := 0.01
		p := 100.0
		if at.After(now.Add(-30 * time.Second)) {
			qty = 40
			if i%3 == 0 {
				p = 100.4
			}
		}
		d.ticks = append(d.ticks, tradeTick{at: at, price: p, qty: qty, buyAggressive: true})
	}
	if reason := d.explainPreTradeReject(now); reason == "" {
		t.Fatal("expected mega_only reject on choppy 30s tape")
	}
}

func TestMatchesFlatMegaProfile(t *testing.T) {
	if !matchesFlatMegaProfile(PreTradeSnap{
		Range60: 0.10, Range30: 0.08, Prior1s: 0.05, Quiet30: 1000, Quiet60: 500, Trades30: 4,
	}) {
		t.Fatal("MLN-like snap should match flat mega")
	}
}

func TestMatchesElevatedMegaProfile(t *testing.T) {
	cfg := BurstConfig{MinQuiet60ElevatedUSDT: 35_000, MinQuiet30ElevatedUSDT: 12_000}
	if !matchesElevatedMegaProfile(cfg, PreTradeSnap{
		Range60: 0.74, Range30: 0.35, Prior1s: 0.26, Quiet60: 38_000, Quiet30: 17_622, Trades60: 200,
	}) {
		t.Fatal("AIGEN-like snap should match elevated mega")
	}
	if matchesElevatedMegaProfile(cfg, PreTradeSnap{
		Range60: 0.48, Range30: 0.22, Prior1s: 0.26, Quiet60: 24_857, Quiet30: 6421, Trades60: 147,
	}) {
		t.Fatal("15 May chop-like snap should not match elevated mega")
	}
}
