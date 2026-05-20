package whale

import (
	"testing"
	"time"
)

func TestPreTradeMegaOnlyFlatProfile(t *testing.T) {
	cfg := BurstConfig{MaxQuiet30UltraUSDT: 120, MaxQuiet30FlatUSDT: 280}
	snap := PreTradeSnap{
		Range60: 0.10, Range30: 0.08, Prior1s: 0.05, Quiet30: 36, Quiet60: 225, Trades30: 2,
	}
	if !matchesUltraFlatMegaProfile(cfg, snap) {
		t.Fatal("MLN-like ultra flat should match")
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
	cfg := BurstConfig{MaxQuiet30UltraUSDT: 120, MaxQuiet30FlatUSDT: 250}
	if !matchesUltraFlatMegaProfile(cfg, PreTradeSnap{
		Range60: 0.10, Range30: 0.08, Prior1s: 0.05, Quiet30: 36, Quiet60: 225, Trades30: 2,
	}) {
		t.Fatal("MLN-like snap should match ultra flat")
	}
	if !matchesStandardFlatMegaProfile(cfg, PreTradeSnap{
		Range60: 0.38, Range30: 0.19, Prior1s: 0.09, Quiet30: 250, Quiet60: 1808, Trades30: 8,
	}) {
		t.Fatal("SYS-like snap should match standard flat")
	}
	if matchesFlatMegaProfile(cfg, PreTradeSnap{
		Range60: 0.11, Range30: 0.09, Prior1s: 0.04, Quiet30: 306, Quiet60: 380, Trades30: 8,
	}) {
		t.Fatal("20 May TURTLE chop should not match flat tiers")
	}
}

func TestLongPreTradeRejectsChop(t *testing.T) {
	cfg := BurstConfig{
		PreTradeEnabled:      true,
		PreTradeMegaOnly:     true,
		PreTradeLongWindowMs: 10_800_000,
		MaxRangeLongPct:      5.5,
		MaxPrior1sLongPct:    0.68,
		MinNotionalLongUSDT:  150_000,
	}
	s := PreTradeSnap{
		Range60: 0.21, Range30: 0.13, Prior1s: 0.11, Quiet30: 164, Quiet60: 1820, Trades30: 6,
		Range2h: 2.23, Prior1s2h: 0.70, Quiet2h: 66_000,
	}
	if reason := explainLongPreTradeReject(cfg, s); reason == "" {
		t.Fatal("TURTLE-like chop should fail min long notional")
	}
	s.Prior1s2h = 0.26
	s.Range2h = 1.92
	s.Quiet2h = 176_000
	if reason := explainLongPreTradeReject(cfg, s); reason != "" {
		t.Fatalf("MLN-like long tape should pass, got %q", reason)
	}
}

func TestElevatedLongRejects2ZChop(t *testing.T) {
	cfg := BurstConfig{PreTradeLongWindowMs: 10_800_000, MaxPrior1sLongElevatedPct: 0.55}
	s := PreTradeSnap{Prior1s2h: 0.76}
	if reason := explainElevatedLongReject(cfg, s); reason == "" {
		t.Fatal("2Z elevated chop should fail long prior_1s")
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
