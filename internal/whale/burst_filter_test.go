package whale

import (
	"strings"
	"testing"
	"time"
)

func TestFastSecRatioFilterRejectReason(t *testing.T) {
	cfg := BurstConfig{
		MinFastSecRatio:    0.50,
		EarlyCaptureAll:    true,
		MinSecNotionalUSDT: 10_000,
		MaxEntrySecMovePct: 0.65,
	}
	d := NewBurstDetector(cfg)
	now := time.Now()
	r := d.explainPumpReject(now, SideBuy, 0.55, 1.20, 5000, 15_000, now.Add(-900*time.Millisecond), now.Add(-100*time.Millisecond))
	if !strings.Contains(r, "fast_sec_ratio") {
		t.Fatalf("reject=%q want fast_sec_ratio", r)
	}
}

func TestFastSecRatioFilterPassesAligned(t *testing.T) {
	cfg := BurstConfig{
		MinFastSecRatio:    0.50,
		EarlyCaptureAll:    true,
		MinSecNotionalUSDT: 10_000,
		MaxEntrySecMovePct: 0.65,
	}
	d := NewBurstDetector(cfg)
	now := time.Now()
	r := d.explainPumpReject(now, SideBuy, 0.55, 0.60, 5000, 15_000, now.Add(-900*time.Millisecond), now.Add(-100*time.Millisecond))
	if r != "" {
		t.Fatalf("unexpected reject: %s", r)
	}
}

func TestFastSecMomentumRatio(t *testing.T) {
	if r := fastSecMomentumRatio(0.17, 1.20); r < 0.13 || r > 0.16 {
		t.Fatalf("TAKE-like ratio=%.2f want ~0.14", r)
	}
	if r := fastSecMomentumRatio(0.55, 1.20); r < 0.45 || r > 0.50 {
		t.Fatalf("late chase ratio=%.2f want ~0.46", r)
	}
	if r := fastSecMomentumRatio(0.90, 1.20); r < 0.70 {
		t.Fatalf("aligned ratio=%.2f want ~0.75", r)
	}
}
