// One-off burst backtest: last N hours on explicit symbols (current whale.yaml rules).
package main

import (
	"flag"
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
	cfgPath := flag.String("config", "config/whale.yaml", "")
	hours := flag.Float64("hours", 1, "replay window hours")
	endStr := flag.String("end", "", "window end RFC3339 (default: now UTC)")
	reverse := flag.Bool("reverse", false, "reverse trade side")
	legacy := flag.Bool("legacy", false, "old loose filters (pre-fix)")
	useWatchlist := flag.Bool("watchlist", false, "first 30 symbols from whale.yaml watchlist")
	verbose := flag.Bool("v", false, "log each signal/exit")
	flag.Parse()

	_ = godotenv.Load()
	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MarginUSDT = 1
	cfg.Leverage = 10
	cfg.ReverseTrade = *reverse
	if *legacy {
		cfg.Burst.EarlyCaptureAll = true
		cfg.Burst.MinSecMovePct = 0.4
		cfg.Burst.MinFastMovePct = 0.5
		cfg.Burst.MinFastSecRatio = 0
		cfg.Burst.MinSecNotionalUSDT = 10000
		cfg.Burst.MaxEntrySecMovePct = 0.70
		cfg.Burst.MinMomentumAlign = 0
		cfg.Burst.MinViolentSecMovePct = 0.70
		cfg.Burst.PreTradeMegaOnly = false
		cfg.Burst.MaxFastMovePct = 0
		cfg.Burst.MinVolumeAccel = 0
		cfg.Burst.MinBurstImpulse = 0
		cfg.Risk.MegaConfirmMinFavorablePct = 2.0
		cfg.Risk.MegaConfirmWindowMs = 60000
		cfg.Risk.MegaTrailActivatePct = 2.0
		cfg.Risk.MegaStopLossPercent = 2.0
		cfg.Risk.MegaTrailMinHoldMs = 2000
	}

	symbols := []string{
		"TAKEUSDT", "GLMUSDT", "TOWNSUSDT", "ELSAUSDT", "HIGHUSDT",
		"SPELLUSDT", "XPINUSDT", "QUSDT", "SKRUSDT", "FRAXUSDT",
	}

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	if *useWatchlist {
		_ = client.WarmSymbolCache()
		perps := client.USDTPerpetualSymbols()
		_ = cfg.ResolveWatchlist(client, perps)
		symbols = cfg.Symbols
		if len(symbols) > 30 {
			symbols = symbols[:30]
		}
	}
	end := time.Now().UTC()
	if *endStr != "" {
		t, err := time.Parse(time.RFC3339, *endStr)
		if err != nil {
			log.Fatal(err)
		}
		end = t.UTC()
	}
	start := end.Add(-time.Duration(*hours * float64(time.Hour)))

	fmt.Printf("burst backtest | %s → %s | margin=%.0f lev=%dx reverse=%v\n",
		start.Format(time.RFC3339), end.Format(time.RFC3339), cfg.MarginUSDT, cfg.Leverage, cfg.ReverseTrade)
	fmt.Printf("filters: sides=%s min_1s=%.1f%% ratio=%.2f reverse=%v entry_delay=%dms slip=%.0fbps\n\n",
		cfg.Burst.SignalSides, cfg.Burst.MinSecMovePct, cfg.Burst.MinFastSecRatio, cfg.ReverseTrade,
		cfg.BacktestEntryDelayMs, cfg.BacktestEntrySlippageBps)
	fmt.Printf("%-12s %8s %8s %8s %8s %s\n", "symbol", "signals", "entries", "pnl", "trades", "exits")

	var tot whale.BacktestSummary
	for _, sym := range symbols {
		trades, err := client.FetchAggTradesRange(sym, start, end)
		if err != nil {
			fmt.Printf("%-12s FETCH ERR: %v\n", sym, err)
			continue
		}
		sum := whale.RunBurstBacktest(cfg, client, sym, trades, *verbose)
		tot.TradesLoaded += sum.TradesLoaded
		tot.Signals += sum.Signals
		tot.Entries += sum.Entries
		tot.ExitsSL += sum.ExitsSL
		tot.ExitsTP1 += sum.ExitsTP1
		tot.ExitsTP2 += sum.ExitsTP2
		tot.ExitsTimeout += sum.ExitsTimeout
		tot.TotalPnLUSDT += sum.TotalPnLUSDT
		fmt.Printf("%-12s %8d %8d %8.2f %8d SL=%d mega=%d no_mega=%d\n",
			sym, sum.Signals, sum.Entries, sum.TotalPnLUSDT, sum.TradesLoaded,
			sum.ExitsSL, sum.ExitsTP2, sum.ExitsTimeout)
	}
	fmt.Printf("\nTOTAL: signals=%d entries=%d pnl=%+.2f USDT (%d symbols, %.1fh)\n",
		tot.Signals, tot.Entries, tot.TotalPnLUSDT, len(symbols), *hours)
}
