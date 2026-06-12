// Download Binance Vision daily aggTrades to data/aggtrades/{date}/{SYMBOL}.json
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

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(0)
	cfgPath := flag.String("config", "config/whale.yaml", "")
	date := flag.String("date", "2026-06-02", "UTC day YYYY-MM-DD")
	out := flag.String("out", binance.DefaultAggDataDir, "output directory")
	symbolsFlag := flag.String("symbols", "", "comma-separated (default: jun2 extreme 8)")
	jun2 := flag.Bool("jun2", false, "download jun2 extreme symbols (same as default list)")
	useWatchlist := flag.Bool("watchlist", false, "first 30 symbols from whale.yaml watchlist")
	limit := flag.Int("limit", 30, "max symbols with -watchlist")
	force := flag.Bool("force", false, "re-download even if local file exists")
	flag.Parse()

	_ = godotenv.Load()

	var symbols []string
	switch {
	case *useWatchlist:
		cfg, err := whale.LoadConfig(*cfgPath)
		if err != nil {
			log.Fatal(err)
		}
		client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
		_ = client.WarmSymbolCache()
		perps := client.USDTPerpetualSymbols()
		if err := cfg.ResolveWatchlist(client, perps); err != nil {
			log.Fatal(err)
		}
		symbols = cfg.Symbols
		if *limit > 0 && len(symbols) > *limit {
			symbols = symbols[:*limit]
		}
	case strings.TrimSpace(*symbolsFlag) != "":
		for _, s := range strings.Split(*symbolsFlag, ",") {
			s = strings.TrimSpace(strings.ToUpper(s))
			if s != "" {
				symbols = append(symbols, s)
			}
		}
	default:
		symbols = append([]string(nil), binance.Jun2ExtremeSymbols...)
	}
	if *jun2 && strings.TrimSpace(*symbolsFlag) == "" && !*useWatchlist {
		symbols = append([]string(nil), binance.Jun2ExtremeSymbols...)
	}

	if len(symbols) == 0 {
		log.Fatal("no symbols")
	}

	fmt.Printf("download aggTrades | date=%s | symbols=%d | out=%s\n\n", *date, len(symbols), *out)
	var ok, fail, skipped int
	var totalTrades int
	t0 := time.Now()

	for i, sym := range symbols {
		path := binance.AggTradeStorePath(*out, *date, sym)
		if !*force {
			if tr, err := binance.LoadAggTradesFile(path); err == nil && len(tr) > 0 {
				fmt.Printf("[%d/%d] %-16s cached %6d trades  %s\n", i+1, len(symbols), sym, len(tr), path)
				skipped++
				totalTrades += len(tr)
				continue
			}
		}
		n, err := binance.DownloadAggTradesDay(sym, *date, *out, *force)
		if err != nil {
			fmt.Printf("[%d/%d] %-16s FAIL  %v\n", i+1, len(symbols), sym, err)
			fail++
			continue
		}
		fmt.Printf("[%d/%d] %-16s saved  %6d trades  %s\n", i+1, len(symbols), sym, n, path)
		ok++
		totalTrades += n
	}

	fmt.Printf("\nDONE: ok=%d cached=%d fail=%d total_trades=%d elapsed=%s\n",
		ok, skipped, fail, totalTrades, time.Since(t0).Round(time.Millisecond))
	if fail > 0 {
		os.Exit(1)
	}
}
