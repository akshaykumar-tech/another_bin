package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	cfgPath := flag.String("config", "", "path to whale.yaml")
	hours := flag.Float64("hours", 24, "hours of history to replay")
	verbose := flag.Bool("verbose", false, "log every FLASH and EXIT")
	sweep := flag.Bool("sweep", false, "try flash parameter variants")
	flag.Parse()

	_ = godotenv.Load()

	whaleCfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	if err := client.WarmSymbolCache(); err != nil {
		log.Fatalf("exchangeInfo: %v", err)
	}

	end := time.Now().UTC()
	start := end.Add(-time.Duration(*hours * float64(time.Hour)))
	ctx := context.Background()

	if *sweep {
		log.Printf("[sweep] flash moves | window %.1fh", *hours)
		results, best, err := whale.RunFullSweep(ctx, whaleCfg, client, start, end)
		if err != nil {
			log.Fatalf("[sweep] failed: %v", err)
		}
		printSweepWinner(results, best)
		return
	}

	perps := client.USDTPerpetualSymbols()
	if err := whaleCfg.ResolveWatchlist(client, perps); err != nil {
		log.Fatalf("watchlist: %v", err)
	}

	log.Printf("[backtest] flash | symbols=%d | 1s>=%.1f%% | $1s>=%.0f",
		len(whaleCfg.Symbols), whaleCfg.Flash.MinSecMovePct, whaleCfg.Flash.MinSecNotionalUSDT)
	log.Printf("[backtest] window %s → %s", start.Format(time.RFC3339), end.Format(time.RFC3339))

	sum, err := whale.RunBacktest(ctx, whaleCfg, client, whale.BacktestOptions{
		Start: start, End: end, Verbose: *verbose,
	})
	if err != nil {
		log.Fatalf("[backtest] failed: %v", err)
	}

	log.Printf("[backtest] ===== SUMMARY =====")
	log.Printf("[backtest] symbols=%d trades=%d signals=%d entries=%d",
		sum.Symbols, sum.TradesLoaded, sum.Signals, sum.Entries)
	log.Printf("[backtest] exits: SL=%d TP1=%d TP2=%d timeout=%d",
		sum.ExitsSL, sum.ExitsTP1, sum.ExitsTP2, sum.ExitsTimeout)
	log.Printf("[backtest] PnL: %.2f USDT (capital=%.0f)", sum.TotalPnLUSDT, whaleCfg.CapitalUSDT)
}

func printSweepWinner(results []whale.SweepResult, best whale.Config) {
	if len(results) == 0 {
		return
	}
	w := results[0]
	for _, r := range results {
		if r.Summary.TotalPnLUSDT > w.Summary.TotalPnLUSDT {
			w = r
		}
	}
	s := w.Summary
	log.Printf("[sweep] BEST: %s | pnl=%+.2f | entries=%d", w.Name, s.TotalPnLUSDT, s.Entries)
	log.Printf("[sweep] config snippet:\n%s", whale.FormatConfigYAML(best))
}
