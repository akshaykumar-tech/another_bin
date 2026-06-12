package whale

import "testing"

// SYS @ 13 May 13:30 IST — measured pre-trade snap (3h window).
func TestSYSMay13PreTradeSnapPasses(t *testing.T) {
	cfg := BurstConfig{
		PreTradeLongWindowMs:      10_800_000,
		MaxRangeLongPct:           5.5,
		MaxPrior1sLongPct:         0.68,
		MinNotionalLongUSDT:       150_000,
		MaxQuiet30UltraUSDT:       120,
		MaxQuiet30FlatUSDT:        260,
		MaxPrior1sLongElevatedPct: 0.55,
	}
	snap := PreTradeSnap{
		Quiet60: 1808, Quiet30: 250, Range60: 0.38, Range30: 0.19, Prior1s: 0.09, Trades30: 8,
		Range2h: 5.35, Prior1s2h: 0.66, Quiet2h: 295_270,
	}
	if !matchesStandardFlatMegaProfile(cfg, snap) {
		t.Fatal("SYS should match standard flat tier")
	}
	if r := explainLongPreTradeReject(cfg, snap); r != "" {
		t.Fatalf("SYS long gate: %s", r)
	}
}
