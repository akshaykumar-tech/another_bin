//go:build integration

package whale

import (
	"os"
	"testing"
	"time"

	"crypto_announcements_go/internal/binance"
)

func TestSYSMay13BurstFiresIntegration(t *testing.T) {
	if os.Getenv("BINANCE_API_KEY") == "" {
		t.Skip("BINANCE_API_KEY not set")
	}
	cfg, err := LoadConfig("../../config/whale.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ist, _ := time.LoadLocation("Asia/Kolkata")
	start := time.Date(2026, 5, 13, 11, 0, 0, 0, ist)
	end := time.Date(2026, 5, 13, 17, 0, 0, 0, ist)
	target := time.Date(2026, 5, 13, 13, 30, 6, 0, ist)

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	trades, err := client.FetchAggTradesRange("SYSUSDT", start.UTC(), end.UTC())
	if err != nil {
		t.Fatal(err)
	}
	diag, max5, max30 := DiagnoseBurstNear(cfg.Burst, trades, target, 90*time.Second)
	t.Logf("max5=%.2f%% max30=%.2f%% fired=%v reject=%q", max5, max30, diag.Fired, diag.RejectReason)

	sum := RunBurstBacktest(cfg, nil, "SYSUSDT", trades, false)
	t.Logf("backtest entries=%d pnl=%.2f", sum.Entries, sum.TotalPnLUSDT)

	pumpFrom := target.Add(-40 * time.Second)
	pumpTo := target.Add(40 * time.Second)
	rep := ReplayBurstPump(cfg, "SYSUSDT", trades, start, pumpFrom, pumpTo, target.Add(3*time.Minute))
	t.Logf("pump replay found=%v reason=%s", rep.Found, rep.ExitReason)

	if sum.Entries == 0 && !rep.Found {
		t.Fatalf("no SYS entry: nearest reject=%q", diag.RejectReason)
	}
}
