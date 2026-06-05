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

func main() {
	log.SetFlags(0)
	_ = godotenv.Load()
	base, err := whale.LoadConfig("config/whale.yaml")
	if err != nil {
		log.Fatal(err)
	}
	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	_ = client.WarmSymbolCache()
	perps := client.USDTPerpetualSymbols()
	if err := base.ResolveWatchlist(client, perps); err != nil {
		log.Fatal(err)
	}
	// 30 thinnest from watchlist
	symbols := base.Symbols
	if len(symbols) > 30 {
		symbols = symbols[:30]
	}

	end := time.Now().UTC()
	start := end.Add(-6 * time.Hour)

	cache := map[string][]binance.AggTrade{}
	for _, sym := range symbols {
		trades, err := client.FetchAggTradesRange(sym, start, end)
		if err != nil {
			continue
		}
		cache[sym] = trades
	}

	type res struct{ name string; ent int; pnl float64 }
	var results []res

	for _, reverse := range []bool{false, true} {
		for _, minSec := range []float64{1.8, 2.0, 2.2} {
			for _, ratio := range []float64{0.65, 0.70, 0.80} {
				for _, noMega := range []float64{0, 0.15, 0.25} {
					for _, sl := range []float64{0.7, 1.0} {
						for _, slip := range []float64{25, 40} {
							cfg := base
							cfg.MarginUSDT, cfg.Leverage = 1, 10
							cfg.ReverseTrade = reverse
							cfg.Burst.EarlyCaptureAll = false
							cfg.Burst.SignalSides = "sell"
							cfg.Burst.MinSecMovePct = minSec
							cfg.Burst.MinViolentSecMovePct = minSec
							cfg.Burst.MinFastSecRatio = ratio
							cfg.Burst.MinFastMovePct = 0.45
							cfg.Burst.MinSecNotionalUSDT = 12000
							cfg.Burst.MaxEntrySecMovePct = 0.65
							cfg.Risk.MegaConfirmMinFavorablePct = noMega
							if noMega == 0 {
								cfg.Risk.MegaConfirmWindowMs = 0
							} else {
								cfg.Risk.MegaConfirmWindowMs = 40000
							}
							cfg.Risk.MegaStopLossPercent = sl
							cfg.Risk.MegaTrailActivatePct = 0.35
							cfg.Risk.MegaTrailMinHoldMs = 2500
							cfg.BacktestEntrySlippageBps = slip
							cfg.BacktestExitSlippageBps = slip

							var ent int
							var pnl float64
							for _, sym := range symbols {
								trades := cache[sym]
								if len(trades) == 0 {
									continue
								}
								s := whale.RunBurstBacktest(cfg, client, sym, trades, false)
								ent += s.Entries
								pnl += s.TotalPnLUSDT
							}
							if ent < 3 {
								continue
							}
							name := fmt.Sprintf("rev=%v sec=%.1f r=%.2f nm=%.2f sl=%.1f slip=%.0f", reverse, minSec, ratio, noMega, sl, slip)
							results = append(results, res{name, ent, pnl})
						}
					}
				}
			}
		}
	}

	// sort top 15 by pnl
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].pnl > results[i].pnl {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
	fmt.Printf("6h backtest | %d symbols | margin=1 lev=10\n", len(symbols))
	fmt.Println("TOP 15 (entries>=3):")
	limit := 15
	if len(results) < limit {
		limit = len(results)
	}
	for i := 0; i < limit; i++ {
		r := results[i]
		fmt.Printf("  %+8.2f USDT  ent=%3d  %s\n", r.pnl, r.ent, r.name)
	}
	if len(results) > 0 {
		b := results[0]
		fmt.Printf("\nBEST: %+8.2f USDT entries=%d\n%s\n", b.pnl, b.ent, b.name)
	}
}
