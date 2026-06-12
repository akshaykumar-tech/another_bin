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

type entry struct {
	label, sym string
	at         time.Time
}

func main() {
	cfgPath := flag.String("config", "config/whale.yaml", "")
	flag.Parse()
	cfg, _ := whale.LoadConfig(*cfgPath)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	_ = client.WarmSymbolCache()

	entries := []entry{
		{"MEGA", "MLNUSDT", time.Date(2026, 5, 13, 13, 30, 6, 0, ist)},
		{"MEGA", "SYSUSDT", time.Date(2026, 5, 13, 13, 30, 6, 0, ist)},
		{"MEGA", "PHBUSDT", time.Date(2026, 5, 13, 13, 30, 6, 0, ist)},
		{"CHOP20", "TURTLEUSDT", time.Date(2026, 5, 20, 4, 34, 39, 0, ist)},
		{"CHOP20", "KMNOUSDT", time.Date(2026, 5, 20, 7, 28, 7, 0, ist)},
		{"CHOP20", "2ZUSDT", time.Date(2026, 5, 20, 5, 58, 6, 0, ist)},
		{"CHOP20", "2ZUSDT", time.Date(2026, 5, 20, 7, 52, 7, 0, ist)},
	}

	longDur := 3 * time.Hour
	fmt.Printf("label sym entry_IST | 3h_notional 3h_range%% 3h_max1s%% | q60 q30 t30 FLAT move30%%\n")
	for _, e := range entries {
		start := e.at.Add(-longDur).UTC()
		end := e.at.Add(35 * time.Second).UTC()
		trades, err := client.FetchAggTradesRange(e.sym, start, end)
		if err != nil {
			log.Printf("skip %s: %v", e.sym, err)
			continue
		}
		longFrom := e.at.Add(-longDur)
		preEnd := e.at.Add(-time.Second)
		qL := notional(trades, longFrom, preEnd)
		rL := rangePct(trades, longFrom, preEnd)
		p1sL := max1s(trades, longFrom, preEnd)
		snap := whale.PreTradeAt(cfg.Burst, trades, e.at.UTC())
		move30 := maxMoveAfter(trades, e.at.UTC(), 30*time.Second)
		fmt.Printf("%-6s %-14s %s | $%9.0f %6.2f%% %6.2f%% | $%5.0f $%4.0f %3d %v %.1f%%\n",
			e.label, e.sym, e.at.Format("15:04:05"),
			qL, rL, p1sL,
			snap.Quiet60, snap.Quiet30, snap.Trades30, snap.FlatMega, move30)
	}
}

func notional(trades []binance.AggTrade, from, to time.Time) float64 {
	var s float64
	for _, t := range trades {
		if !t.Time.Before(from) && !t.Time.After(to) {
			s += t.Price * t.Quantity
		}
	}
	return s
}

func tradeCount(trades []binance.AggTrade, from, to time.Time) int {
	n := 0
	for _, t := range trades {
		if !t.Time.Before(from) && !t.Time.After(to) {
			n++
		}
	}
	return n
}

func rangePct(trades []binance.AggTrade, from, to time.Time) float64 {
	hi, lo := 0.0, math.MaxFloat64
	for _, t := range trades {
		if t.Time.Before(from) || t.Time.After(to) {
			continue
		}
		if t.Price > hi {
			hi = t.Price
		}
		if t.Price < lo {
			lo = t.Price
		}
	}
	if lo <= 0 || hi <= 0 || lo == math.MaxFloat64 {
		return 0
	}
	mid := (hi + lo) / 2
	return (hi - lo) / mid * 100
}

func max1s(trades []binance.AggTrade, from, to time.Time) float64 {
	best := 0.0
	for i := 0; i < len(trades); i++ {
		if trades[i].Time.Before(from) || trades[i].Time.After(to) {
			continue
		}
		p0 := trades[i].Price
		if p0 <= 0 {
			continue
		}
		end := trades[i].Time.Add(time.Second)
		for j := i; j < len(trades) && !trades[j].Time.After(end); j++ {
			if trades[j].Time.After(to) {
				break
			}
			pct := math.Abs((trades[j].Price - p0) / p0 * 100)
			if pct > best {
				best = pct
			}
		}
	}
	return best
}

func maxMoveAfter(trades []binance.AggTrade, from time.Time, w time.Duration) float64 {
	p0 := 0.0
	for _, tr := range trades {
		if !tr.Time.After(from) {
			p0 = tr.Price
		}
	}
	if p0 <= 0 {
		return 0
	}
	best := 0.0
	for _, tr := range trades {
		dt := tr.Time.Sub(from)
		if dt < 0 || dt > w {
			continue
		}
		ch := math.Abs((tr.Price - p0) / p0 * 100)
		if ch > best {
			best = ch
		}
	}
	return best
}
