// One-off: compare tape context at violent dump signals.
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

type target struct {
	sym string
	at  string
}

func main() {
	log.SetFlags(0)
	_ = godotenv.Load()
	cfg, err := whale.LoadConfig("config/whale.yaml")
	if err != nil {
		log.Fatal(err)
	}
	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))

	targets := []target{
		{"TAKEUSDT", "2026-06-05T02:56:10Z"},
		{"QUSDT", "2026-06-05T06:13:08Z"},
		{"BROCCOLIF3BUSDT", "2026-06-05T06:23:25Z"},
		{"KOMAUSDT", "2026-06-05T06:36:07Z"},
	}
	end := time.Now().UTC()
	start := end.Add(-8 * time.Hour)

	for _, t := range targets {
		at, err := time.Parse(time.RFC3339, t.at)
		if err != nil {
			log.Fatal(err)
		}
		trades, err := client.FetchAggTradesRange(t.sym, start, end)
		if err != nil {
			fmt.Printf("%s: fetch err %v\n", t.sym, err)
			continue
		}
		diag, max5, max30 := whale.DiagnoseBurstNearFast(cfg.Burst, trades, at, 3*time.Second)
		fmt.Printf("\n=== %s @ %s ===\n", t.sym, t.at)
		fmt.Printf("  fired=%v side=%s fast=%.2f%% sec=%.2f%% vol=$%.0f\n",
			diag.Fired, diag.Side, diag.FastMove, diag.SecMove, diag.SecNotional)
		fmt.Printf("  reject=%q max5=%.2f%% max30=%.2f%%\n", diag.RejectReason, max5, max30)
	}
}
