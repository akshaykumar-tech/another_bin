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
	symbol := flag.String("symbol", "MLNUSDT", "")
	startStr := flag.String("start", "2026-05-13T11:00:00+05:30", "")
	endStr := flag.String("end", "2026-05-13T17:00:00+05:30", "")
	cfgPath := flag.String("config", "config/whale.yaml", "")
	flag.Parse()

	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, *startStr)
	end, _ := time.Parse(time.RFC3339, *endStr)
	ist, _ := time.LoadLocation("Asia/Kolkata")

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	if err := client.WarmSymbolCache(); err != nil {
		log.Fatal(err)
	}
	trades, err := client.FetchAggTradesRange(*symbol, start.UTC(), end.UTC())
	if err != nil {
		log.Fatal(err)
	}

	det := whale.NewBurstDetector(cfg.Burst)
	type row struct {
		at     time.Time
		side   whale.Side
		vol    float64
		move30 float64
		snap   whale.PreTradeSnap
	}
	var entries []row

	for _, tr := range trades {
		sig := det.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
		if sig == nil {
			continue
		}
		snap := whale.PreTradeAt(cfg.Burst, trades, tr.Time)
		move30 := maxMoveAfter(trades, tr.Time, 30*time.Second)
		entries = append(entries, row{tr.Time, sig.Side, sig.SecVolume, move30, snap})
	}

	fmt.Printf("=== %s | %s → %s IST | entries=%d ===\n\n",
		*symbol, start.In(ist).Format("2006-01-02 15:04"), end.In(ist).Format("15:04"), len(entries))
	fmt.Printf("%-12s %-5s %8s %7s %7s %7s %6s %5s %5s %5s %s\n",
		"entry_IST", "side", "vol1s", "q60", "q30", "r60", "r30", "p1s", "t60", "t30", "profile")
	for _, e := range entries {
		prof := "—"
		if e.snap.FlatMega {
			prof = "FLAT"
		} else if e.snap.ElevatedMega {
			prof = "ELEV"
		}
		fmt.Printf("%s %-5s $%7.0f $%6.0f $%5.0f %5.2f%% %5.2f%% %4.2f%% %4d %4d %-4s move30=%.1f%%\n",
			e.at.In(ist).Format("15:04:05"), e.side, e.vol,
			e.snap.Quiet60, e.snap.Quiet30, e.snap.Range60, e.snap.Range30, e.snap.Prior1s,
			e.snap.Trades60, e.snap.Trades30, prof, e.move30)
	}
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
	var best float64
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
