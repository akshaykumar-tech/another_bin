// Backtest cached Vision days with margin=1 lev=10 (live-like sizing).
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

func main() {
	log.SetFlags(0)
	cfgPath := flag.String("config", "config/whale.yaml", "")
	datesFlag := flag.String("dates", "2026-05-31,2026-06-01,2026-06-02,2026-06-03,2026-06-04", "")
	symbolsFlag := flag.String("symbols", "", "comma-separated (default jun2 extreme 8)")
	dataDir := flag.String("data-dir", binance.DefaultAggDataDir, "")
	reverse := flag.Bool("reverse", false, "")
	perSym := flag.Bool("per-symbol", false, "show per-symbol totals")
	maxTrades := flag.Int("max-trades", 250000, "")
	flag.Parse()

	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MarginUSDT = 1
	cfg.Leverage = 10
	cfg.ReverseTrade = *reverse

	var symbols []string
	if strings.TrimSpace(*symbolsFlag) != "" {
		for _, s := range strings.Split(*symbolsFlag, ",") {
			s = strings.TrimSpace(strings.ToUpper(s))
			if s != "" {
				symbols = append(symbols, s)
			}
		}
	} else {
		symbols = append([]string(nil), binance.Jun2ExtremeSymbols...)
	}

	dir := "same"
	if *reverse {
		dir = "reverse"
	}
	fmt.Printf("multi-day | %s | margin=1 lev=10 | slip=%.0fbps delay=%d/%dms\n",
		dir, cfg.BacktestEntrySlippageBps, cfg.BacktestEntryDelayMs, cfg.BacktestExitDelayMs)
	fmt.Printf("violent_scalp=%v stall=%dms | min_sec=%.1f%% notional=%.0f\n\n",
		cfg.Risk.ViolentScalpEnabled, cfg.Risk.ViolentScalpStallMs,
		cfg.Burst.MinSecMovePct, cfg.Burst.MinSecNotionalUSDT)

	var totalPnL float64
	var totalEnt, totalSig int
	symPnL := map[string]float64{}
	symEnt := map[string]int{}

	for _, date := range strings.Split(*datesFlag, ",") {
		date = strings.TrimSpace(date)
		if date == "" {
			continue
		}
		var dayPnL float64
		var dayEnt int
		for _, sym := range symbols {
			trades, _, err := binance.LoadAggTradesDay(sym, date, *dataDir, nil, false)
			if err != nil || (*maxTrades > 0 && len(trades) > *maxTrades) {
				continue
			}
			sum := whale.RunBurstBacktest(cfg, nil, sym, trades, false)
			dayPnL += sum.TotalPnLUSDT
			dayEnt += sum.Entries
			totalSig += sum.Signals
			symPnL[sym] += sum.TotalPnLUSDT
			symEnt[sym] += sum.Entries
		}
		totalPnL += dayPnL
		totalEnt += dayEnt
		fmt.Printf("%s  ent=%3d  pnl=%+7.2f USDT\n", date, dayEnt, dayPnL)
	}
	fmt.Printf("\nTOTAL ent=%d sig=%d pnl=%+7.2f USDT\n", totalEnt, totalSig, totalPnL)
	if *perSym {
		fmt.Println("\nper-symbol:")
		for _, sym := range symbols {
			fmt.Printf("  %-14s ent=%3d pnl=%+7.2f\n", sym, symEnt[sym], symPnL[sym])
		}
	}
}
