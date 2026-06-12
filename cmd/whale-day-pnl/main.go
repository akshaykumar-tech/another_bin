package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

func main() {
	cfgPath := flag.String("config", "config/whale.yaml", "")
	date := flag.String("date", "2026-06-03", "UTC day YYYY-MM-DD")
	symbolsFlag := flag.String("symbols", "", "comma-separated (required)")
	maxTrades := flag.Int("max-trades", 200000, "skip if more aggTrades")
	useAPI := flag.Bool("use-api", false, "fetch aggTrades from Binance API (today / no Vision ZIP)")
	dataDir := flag.String("data-dir", binance.DefaultAggDataDir, "local aggTrades cache (preferred over Vision)")
	reverse := flag.Bool("reverse", false, "reverse_trade: signal BUY → trade SELL (same burst logic)")
	ideal := flag.Bool("ideal", false, "also run ideal no-entry-delay/no-slip comparison")
	flag.Parse()

	if strings.TrimSpace(*symbolsFlag) == "" {
		log.Fatal("use --symbols SYM1,SYM2,...")
	}

	base, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	paper := base
	real := base

	if *reverse {
		paper.ReverseTrade = true
		real.ReverseTrade = true
	}
	if *ideal {
		// Ideal comparison mode: entry delay and slippage removed.
		paper.BacktestEntryDelayMs = 0
		paper.BacktestExitDelayMs = 0
		paper.BacktestEntrySlippageBps = 0
		paper.BacktestExitSlippageBps = 0
	}

	var symbols []string
	for _, s := range strings.Split(*symbolsFlag, ",") {
		s = strings.TrimSpace(strings.ToUpper(s))
		if s != "" {
			symbols = append(symbols, s)
		}
	}

	var client *binance.FuturesClient
	if *useAPI {
		client = binance.NewFuturesClient(
			"https://fapi.binance.com",
			os.Getenv("BINANCE_API_KEY"),
			os.Getenv("BINANCE_API_SECRET"),
		)
	}
	dataSrc := "local→vision"
	if *useAPI {
		dataSrc = "api"
	}
	fmt.Printf("=== %s | early_capture_all=%v | reverse_trade=%v | %d symbols | data=%s ===\n\n",
		*date, base.Burst.EarlyCaptureAll, *reverse, len(symbols), dataSrc)
	if *ideal {
		fmt.Printf("%-14s %8s %8s %10s %10s %8s\n", "symbol", "entries", "signals", "PnL ideal", "PnL real", "Δ USDT")
	} else {
		fmt.Printf("%-14s %8s %8s %10s\n", "symbol", "entries", "signals", "PnL real")
	}
	fmt.Println(strings.Repeat("-", 62))

	var totPaper, totReal float64
	var totEnt int

	for i, sym := range symbols {
		if *useAPI && i > 0 {
			time.Sleep(400 * time.Millisecond)
		}
		trades, src, err := binance.LoadAggTradesDay(sym, *date, *dataDir, client, *useAPI)
		_ = src
		if err != nil {
			fmt.Printf("%-14s ERR  %v\n", sym, err)
			continue
		}
		if *maxTrades > 0 && len(trades) > *maxTrades {
			fmt.Printf("%-14s SKIP >%d trades\n", sym, *maxTrades)
			continue
		}
		if len(trades) < 100 {
			fmt.Printf("%-14s SKIP too few trades (%d)\n", sym, len(trades))
			continue
		}

		sr := whale.RunBurstBacktest(real, nil, sym, trades, false)
		totReal += sr.TotalPnLUSDT
		if *ideal {
			sp := whale.RunBurstBacktest(paper, nil, sym, trades, false)
			totPaper += sp.TotalPnLUSDT
			totEnt += sp.Entries
			fmt.Printf("%-14s %8d %8d %10.2f %10.2f %8.2f\n",
				sym, sp.Entries, sp.Signals, sp.TotalPnLUSDT, sr.TotalPnLUSDT, sr.TotalPnLUSDT-sp.TotalPnLUSDT)
		} else {
			totEnt += sr.Entries
			fmt.Printf("%-14s %8d %8d %10.2f\n",
				sym, sr.Entries, sr.Signals, sr.TotalPnLUSDT)
		}
	}

	fmt.Println(strings.Repeat("-", 62))
	if *ideal {
		fmt.Printf("%-14s %8d %8s %10.2f %10.2f %8.2f\n", "TOTAL", totEnt, "", totPaper, totReal, totReal-totPaper)
		fmt.Printf("\nideal = signal tick entry, no delay + no slip\n")
	}
	fmt.Printf("real   = entry %dms + exit %dms + slip %.0f/%.0f bps\n",
		real.BacktestEntryDelayMs, real.BacktestExitDelayMs,
		real.BacktestEntrySlippageBps, real.BacktestExitSlippageBps)
}
