package whale

import (
	"testing"
	"time"
)

func TestViolentScalpStallExit(t *testing.T) {
	r := Risk{
		ViolentScalpEnabled: true, ViolentScalpMinMovePct: 1.8,
		ViolentScalpStallMs: 20_000, ViolentScalpStallMinPct: 0.18,
		MegaStopLossPercent: 1.5,
	}
	pos := &simPosition{
		Symbol: "TEST", Side: SideSell, EntryPrice: 1.0,
		MarginUSDT: 1, Leverage: 10, OpenedAt: time.Now(),
		PeakPrice: 1.0, SignalAbsMovePct: 2.0, MegaExit: true,
	}
	// flat after 20s → stall
	closed, reason, _ := violentScalpExitStep(r, pos, 1.0, pos.OpenedAt.Add(21*time.Second))
	if !closed || reason != "stall" {
		t.Fatalf("want stall, got closed=%v reason=%s", closed, reason)
	}
}

func TestViolentScalpTrailOnContinuation(t *testing.T) {
	r := Risk{
		ViolentScalpEnabled: true, ViolentScalpMinMovePct: 1.8,
		ViolentScalpTrailActivatePct: 0.25, ViolentScalpTrailDistPct: 0.12,
		ViolentScalpStallMs: 60_000,
	}
	pos := &simPosition{
		Symbol: "TEST", Side: SideSell, EntryPrice: 1.0,
		MarginUSDT: 1, Leverage: 10, OpenedAt: time.Now(),
		PeakPrice: 0.975, SignalAbsMovePct: 2.0, MegaExit: true,
	}
	// still near lows — trail not triggered
	closed, reason, _ := violentScalpExitStep(r, pos, 0.974, pos.OpenedAt.Add(5*time.Second))
	if closed {
		t.Fatalf("continuation should hold, closed=%v reason=%s", closed, reason)
	}
}
