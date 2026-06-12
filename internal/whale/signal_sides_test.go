package whale

import "testing"

func TestAllowsSignalSide(t *testing.T) {
	c := Config{Burst: BurstConfig{SignalSides: "sell"}}
	if c.AllowsSignalSide(SideBuy) {
		t.Fatal("buy should be blocked")
	}
	if !c.AllowsSignalSide(SideSell) {
		t.Fatal("sell should pass")
	}
}
