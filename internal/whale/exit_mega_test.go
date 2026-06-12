package whale

import (
	"testing"
	"time"
)

func TestMegaStopLossScalesWithSignal(t *testing.T) {
	r := Risk{MegaStopLossPercent: 1.0, MegaSLSignalRatio: 0.75, MegaSLSignalMinMovePct: 2.0}
	pos := &simPosition{SignalAbsMovePct: 2.0}
	if got := megaStopLossPct(r, pos) * 100; got != 1.5 {
		t.Fatalf("scaled SL=%.2f%% want 1.5%%", got)
	}
	pos.SignalAbsMovePct = 1.0
	if got := megaStopLossPct(r, pos) * 100; got != 1.0 {
		t.Fatalf("small signal SL=%.2f%% want 1.0%%", got)
	}
}

func TestMegaExitSLRespectsMinHold(t *testing.T) {
	r := Risk{MegaStopLossPercent: 1.0, MegaTrailMinHoldMs: 3000}
	pos := &simPosition{
		Symbol: "TESTUSDT", Side: SideSell, EntryPrice: 1.0,
		MarginUSDT: 1, Leverage: 10, OpenedAt: time.Now(), PeakPrice: 1.0,
	}
	// +1.5% adverse for short at t=0 — should NOT stop yet
	closed, reason, _ := megaExitStep(r, pos, 1.015, time.Now())
	if closed || reason == "sl" {
		t.Fatalf("expected no SL before hold grace, got closed=%v reason=%s", closed, reason)
	}
	// same move after 3s — should SL
	closed, reason, _ = megaExitStep(r, pos, 1.015, pos.OpenedAt.Add(3*time.Second))
	if !closed || reason != "sl" {
		t.Fatalf("expected sl after grace, got closed=%v reason=%s", closed, reason)
	}
}
