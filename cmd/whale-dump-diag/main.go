package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

func main() {
	symbol := flag.String("symbol", "COOKIEUSDT", "")
	targetStr := flag.String("at", "2026-05-22T13:30:00+05:30", "target IST")
	windowMin := flag.Int("window", 10, "minutes each side")
	flag.Parse()

	cfg, err := whale.LoadConfig("config/whale.yaml")
	if err != nil {
		log.Fatal(err)
	}
	ist, _ := time.LoadLocation("Asia/Kolkata")
	target, err := time.ParseInLocation("2006-01-02T15:04:05-07:00", *targetStr, ist)
	if err != nil {
		target, err = time.Parse(time.RFC3339, *targetStr)
		if err != nil {
			log.Fatal(err)
		}
	}
	start := target.Add(-time.Duration(*windowMin) * time.Minute)
	end := target.Add(time.Duration(*windowMin) * time.Minute)

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	trades, err := client.FetchAggTradesRange(*symbol, start.UTC(), end.UTC())
	if err != nil {
		log.Fatal(err)
	}

	max1s, at1s := max1sMoveInWindow(trades, start, end)
	diag, _, _ := whale.DiagnoseBurstNear(cfg.Burst, trades, target, 3*time.Minute)

	fmt.Printf("=== %s | target %s IST | ±%dm ===\n", *symbol, target.In(ist).Format("15:04:05"), *windowMin)
	fmt.Printf("  aggTrades=%d | max 1s move in window: %.2f%% @ %s IST\n", len(trades), max1s, at1s.In(ist).Format("15:04:05.000"))
	if diag.Fired {
		fmt.Printf("  burst FIRED @ %s: %s fast=%.2f%% 1s=%.2f%% vol=$%.0f\n",
			diag.At.In(ist).Format("15:04:05.000"), diag.Side, diag.FastMove, diag.SecMove, diag.SecNotional)
	} else if diag.RejectReason != "" {
		fmt.Printf("  nearest block @ %s: %s\n", diag.At.In(ist).Format("15:04:05.000"), diag.RejectReason)
	} else {
		fmt.Printf("  no burst candidate ±3m (1s move may never hit 0.4-0.7%% band in one tick)\n")
	}
	snap := whale.PreTradeAt(cfg.Burst, trades, target)
	fmt.Printf("  pretrade @ target: q60=$%.0f q30=$%.0f ultra=%v flat=%v elev=%v reject=%q\n",
		snap.Quiet60, snap.Quiet30, snap.FlatUltra, snap.FlatStandard, snap.ElevatedMega, snap.RejectReason)

	if at1s.After(start) && !at1s.After(end) {
		snapAt := whale.PreTradeAt(cfg.Burst, trades, at1s)
		fmt.Printf("  pretrade @ max1s: q2h=$%.0f range2h=%.2f%% prior1s2h=%.2f%% violent_reject=%q\n",
			snapAt.Quiet2h, snapAt.Range2h, snapAt.Prior1s2h,
			whale.ExplainViolentPreTradeReject(cfg.Burst, snapAt))
	}

	d := whale.NewBurstDetector(cfg.Burst)
	var fired int
	for _, tr := range trades {
		if sig := d.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time); sig != nil {
			fired++
			fmt.Printf("  LIVE SCAN fired #%d @ %s: %s 1s=%.2f%% fast=%.2f%% vol=$%.0f\n",
				fired, tr.Time.In(ist).Format("15:04:05.000"), sig.Side, sig.MovePct, sig.FastMove, sig.SecVolume)
		}
	}
	if fired == 0 {
		fmt.Printf("  LIVE SCAN: 0 signals in window (last reject=%q)\n", d.LastPumpReject())
	}
}

func max1sMoveInWindow(trades []binance.AggTrade, start, end time.Time) (float64, time.Time) {
	var best float64
	var bestAt time.Time
	for i := 0; i < len(trades); i++ {
		if trades[i].Time.Before(start) || trades[i].Time.After(end) {
			continue
		}
		p0 := trades[i].Price
		if p0 <= 0 {
			continue
		}
		tEnd := trades[i].Time.Add(time.Second)
		for j := i; j < len(trades) && !trades[j].Time.After(tEnd); j++ {
			if trades[j].Time.Before(start) || trades[j].Time.After(end) {
				continue
			}
			pct := math.Abs((trades[j].Price - p0) / p0 * 100)
			if pct > best {
				best = pct
				bestAt = trades[i].Time
			}
		}
	}
	return best, bestAt
}
