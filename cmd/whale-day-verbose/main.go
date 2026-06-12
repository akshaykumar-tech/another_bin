// Verbose burst backtest for one symbol/day from local Vision cache.
package main

import (
	"flag"
	"fmt"
	"log"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

func main() {
	log.SetFlags(0)
	cfgPath := flag.String("config", "config/whale.yaml", "")
	date := flag.String("date", "2026-06-02", "")
	sym := flag.String("symbol", "", "required")
	dataDir := flag.String("data-dir", binance.DefaultAggDataDir, "")
	flag.Parse()
	if *sym == "" {
		log.Fatal("-symbol required")
	}
	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MarginUSDT = 1
	cfg.Leverage = 10
	trades, src, err := binance.LoadAggTradesDay(*sym, *date, *dataDir, nil, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s | %d trades | source=%s | slip=%.0fbps delay=%d/%dms\n\n",
		*date, *sym, len(trades), src, cfg.BacktestEntrySlippageBps,
		cfg.BacktestEntryDelayMs, cfg.BacktestExitDelayMs)
	sum := whale.RunBurstBacktest(cfg, nil, *sym, trades, true)
	fmt.Printf("\nTOTAL: signals=%d entries=%d pnl=%+.2f SL=%d trail=%d timeout=%d\n",
		sum.Signals, sum.Entries, sum.TotalPnLUSDT, sum.ExitsSL, sum.ExitsTP2, sum.ExitsTimeout)
}
