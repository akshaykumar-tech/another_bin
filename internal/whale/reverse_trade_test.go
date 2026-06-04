package whale

import "testing"

func TestTradeSideReverse(t *testing.T) {
	c := Config{ReverseTrade: true}
	if got := c.TradeSide(SideBuy); got != SideSell {
		t.Fatalf("TradeSide(BUY)=%s want SELL", got)
	}
	if got := c.TradeSide(SideSell); got != SideBuy {
		t.Fatalf("TradeSide(SELL)=%s want BUY", got)
	}
}

func TestRiskForExitReverseSL(t *testing.T) {
	c := Config{
		ReverseTrade:       true,
		ReverseStopLossPct: 1.5,
		Risk: Risk{MegaStopLossPercent: 2.0, StopLossPercent: 2.0},
	}
	r := c.RiskForExit()
	if r.MegaStopLossPercent != 1.5 || r.StopLossPercent != 1.5 {
		t.Fatalf("RiskForExit SL=%.2f/%.2f want 1.5", r.MegaStopLossPercent, r.StopLossPercent)
	}
}
