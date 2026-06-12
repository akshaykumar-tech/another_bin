// Replay today's live dry-run signals with realistic backtest (delay + slip).
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"

	"github.com/joho/godotenv"
)

type liveSignal struct {
	sym      string
	at       string
	dryEntry float64
	dryExit  float64
	dryPnL   float64
	dryReason string
}

func main() {
	log.SetFlags(0)
	_ = godotenv.Load()

	signals := []liveSignal{
		{"TAKEUSDT", "2026-06-05T10:40:06Z", 0.016622, 0.015408, 36.52, "scalp_tp"},
		{"SIGNUSDT", "2026-06-05T10:44:48Z", 0.009065, 0.009249, -10.12, "sl"},
		{"HAEDALUSDT", "2026-06-05T10:47:06Z", 0.021819, 0.021671, 3.40, "trail"},
		{"BTRUSDT", "2026-06-05T10:52:47Z", 0.019930, 0.019780, 3.77, "trail"},
		{"NFPUSDT", "2026-06-05T11:12:27Z", 0.008416, 0.008496, -4.75, "stall"},
	}

	base, err := whale.LoadConfig("config/whale.yaml")
	if err != nil {
		log.Fatal(err)
	}
	base.ReverseTrade = false
	base.MarginUSDT = 50
	base.Leverage = 10
	base.CooldownSec = 0 // isolate each signal window

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))

	type profile struct {
		name      string
		entryMs   int
		exitMs    int
		entryBps  float64
		exitBps   float64
	}
	profiles := []profile{
		{"dry (0ms 5bps)", 0, 0, 5, 5},
		{"live~30ms 40bps", 30, 10, 40, 40},
		{"backtest (200ms 40bps)", 200, 10, 40, 40},
	}

	fmt.Printf("replay %d live signals | margin=50 lev=10 | reverse=false\n\n", len(signals))
	fmt.Printf("%-12s %-8s %-22s %8s %8s %8s %s\n",
		"symbol", "profile", "window", "signals", "entries", "pnl", "note")
	fmt.Println("--------------------------------------------------------------------------------")

	var totals [3]float64
	for _, sig := range signals {
		at, err := time.Parse(time.RFC3339, sig.at)
		if err != nil {
			log.Fatal(err)
		}
		start := at.Add(-90 * time.Second)
		end := at.Add(150 * time.Second)

		trades, err := client.FetchAggTradesRange(sig.sym, start, end)
		if err != nil {
			fmt.Printf("%-12s FETCH ERR: %v\n", sig.sym, err)
			continue
		}

		for pi, p := range profiles {
			cfg := base
			cfg.BacktestEntryDelayMs = p.entryMs
			cfg.BacktestExitDelayMs = p.exitMs
			cfg.BacktestEntrySlippageBps = p.entryBps
			cfg.BacktestExitSlippageBps = p.exitBps

			sum := whale.RunBurstBacktest(cfg, client, sig.sym, trades, true)
			note := ""
			if sum.Entries == 0 {
				note = "no entry (continuation/bounce/cooldown)"
			}
			fmt.Printf("%-12s %-8s %s→%s %8d %8d %8.2f %s\n",
				sig.sym, p.name,
				start.Format("15:04:05"), end.Format("15:04:05"),
				sum.Signals, sum.Entries, sum.TotalPnLUSDT, note)
			totals[pi] += sum.TotalPnLUSDT
		}
		fmt.Printf("%-12s %-8s dry-run was: entry=%.6f exit=%.6f pnl=%+.2f reason=%s\n\n",
			sig.sym, "COMPARE", sig.dryEntry, sig.dryExit, sig.dryPnL, sig.dryReason)
		time.Sleep(300 * time.Millisecond)
	}

	fmt.Println("--------------------------------------------------------------------------------")
	for i, p := range profiles {
		fmt.Printf("TOTAL %-22s %+.2f USDT\n", p.name, totals[i])
	}
}
