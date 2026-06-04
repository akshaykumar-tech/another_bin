package whale

import (
	"testing"
	"time"
)

func TestViolentBurstBypassesFastMoveGate(t *testing.T) {
	cfg := BurstConfig{
		FastWindowMs:            100,
		SecWindowMs:             1000,
		MinFastMovePct:          0.5,
		MinSecMovePct:           0.4,
		MinSecNotionalUSDT:      12_000,
		MinFastNotionalUSDT:     3_000,
		PumpOnly:                true,
		ViolentBurstEnabled:     true,
		MinViolentSecMovePct:    2.0,
		MaxViolentSecMovePct:    16.0,
		MinViolentSecNotionalUSDT: 12_000,
		MaxEntrySecMovePct:      0.7,
		PreTradeEnabled:         false,
		SideDominancePct:        52,
	}
	d := NewBurstDetector(cfg)
	base := time.Date(2026, 5, 22, 8, 0, 0, 0, time.UTC)
	// Slow 100ms ramp but ~9% in 1s (22 May COOKIE-style dump leg).
	prices := []float64{1.000, 0.995, 0.990, 0.985, 0.978, 0.970, 0.960, 0.948, 0.935, 0.910}
	for i, p := range prices {
		at := base.Add(time.Duration(i*100) * time.Millisecond)
		d.OnAggTrade(p, 2500, true, at) // seller taker → dump
	}
	sig := d.OnAggTrade(0.910, 2500, true, base.Add(900*time.Millisecond))
	if sig == nil {
		t.Fatalf("expected violent burst signal, last reject=%q", d.lastPumpReject)
	}
	if sig.Side != SideSell {
		t.Fatalf("side=%s want SELL", sig.Side)
	}
	if sig.MovePct >= 0 {
		t.Fatalf("move=%.2f want negative dump", sig.MovePct)
	}
}

func TestEarlyCaptureAllSkipsViolentPreTradeGates(t *testing.T) {
	cfg := BurstConfig{EarlyCaptureAll: true}
	cfg.applyEarlyCaptureAll()
	s := PreTradeSnap{Range2h: 10.15, Prior1s: 3.97, Quiet60: 115_000}
	if r := ExplainViolentPreTradeReject(cfg, s); r != "" {
		t.Fatalf("early capture should not reject: %s", r)
	}
	d := NewBurstDetector(cfg)
	if !d.passesPreTradeForBurst(time.Now(), 6.0, 15_000) {
		t.Fatalf("pre_trade should pass, reject=%q", d.lastPumpReject)
	}
}

func TestViolentPreTradeUsesRelaxedLongGates(t *testing.T) {
	cfg := BurstConfig{
		ViolentBurstEnabled:        true,
		MinViolentSecMovePct:       2,
		MinViolentSecNotionalUSDT:  12_000,
		PreTradeLongWindowMs:       7_200_000,
		MaxRangeLongViolentPct:     9,
		MaxPrior1sLongViolentPct:   1.2,
		MinNotionalLongViolentUSDT: 30_000,
	}
	s := PreTradeSnap{Range2h: 4, Prior1s: 0.3, Prior1s2h: 1.5, Quiet2h: 50_000}
	if r := ExplainViolentPreTradeReject(cfg, s); r != "" {
		t.Fatalf("unexpected reject: %s", r)
	}
}
