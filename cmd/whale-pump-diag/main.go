package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

func main() {
	cfg, err := whale.LoadConfig("config/whale.yaml")
	if err != nil {
		log.Fatal(err)
	}
	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	_ = client.WarmSymbolCache()

	ist, _ := time.LoadLocation("Asia/Kolkata")
	day := time.Date(2026, 5, 13, 0, 0, 0, 0, ist)
	start := time.Date(day.Year(), day.Month(), day.Day(), 11, 0, 0, 0, ist)
	end := time.Date(day.Year(), day.Month(), day.Day(), 17, 0, 0, 0, ist)
	target := time.Date(day.Year(), day.Month(), day.Day(), 13, 30, 6, 0, ist)

	symbols := []string{"ATAUSDT", "PHBUSDT", "SYSUSDT", "MLNUSDT", "AIGENSYNUSDT"}
	fmt.Println("=== 13 May 2026 pump diagnostic @ 13:30:06 IST ===")
	fmt.Println()

	for _, sym := range symbols {
		trades, err := client.FetchAggTradesRange(sym, start.UTC(), end.UTC())
		if err != nil {
			fmt.Printf("%s: fetch error: %v\n\n", sym, err)
			continue
		}
		diag, max5, max30 := whale.DiagnoseBurstNear(cfg.Burst, trades, target, 90*time.Second)

		pumpFrom := target.Add(-40 * time.Second)
		pumpTo := target.Add(40 * time.Second)
		warmup := start
		pumpEnd := target.Add(3 * time.Minute)
		rep := whale.ReplayBurstPump(cfg, sym, trades, warmup, pumpFrom, pumpTo, pumpEnd)

		fmt.Printf("## %s\n", sym)
		fmt.Printf("  aggTrades: %d | max move after 13:30:06: 5s=%.2f%% 30s=%.2f%%\n", len(trades), max5, max30)
		if rep.Found {
			fmt.Printf("  pump window entry: %s IST @ %.6f → exit %s capture=%.2f%% peak=%.2f%% reason=%s\n",
				rep.EntryTime.In(ist).Format("15:04:05.000"), rep.EntryPrice,
				rep.ExitTime.In(ist).Format("15:04:05.000"), rep.CapturePct, rep.PeakPct, rep.ExitReason)
		} else {
			fmt.Printf("  pump window: NO entry 13:29:26–13:30:46 IST\n")
		}
		if diag.Fired {
			fmt.Printf("  nearest signal (±90s): %s IST FIRED %s fast=%.2f%% 1s=%.2f%% vol=$%.0f\n",
				diag.At.In(ist).Format("15:04:05.000"), diag.Side, diag.FastMove, diag.SecMove, diag.SecNotional)
		} else if diag.RejectReason != "" {
			fmt.Printf("  nearest probe (±90s): %s IST blocked: %s\n", diag.At.In(ist).Format("15:04:05.000"), diag.RejectReason)
		} else {
			fmt.Printf("  nearest probe: no burst candidate within ±90s (move/volume too small at probe)\n")
		}
		fmt.Println()
	}
}
