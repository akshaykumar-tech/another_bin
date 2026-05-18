package main

import (
	"fmt"
	"os"
	"strings"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	if err := client.WarmSymbolCache(); err != nil {
		panic(err)
	}
	perps := client.USDTPerpetualSymbols()
	cfg, err := whale.LoadConfig("config/whale.yaml")
	if err != nil {
		panic(err)
	}
	if err := cfg.ResolveWatchlist(client, perps); err != nil {
		panic(err)
	}
	target := "MLNUSDT"
	if len(os.Args) > 1 {
		target = strings.ToUpper(os.Args[1])
	}
	found := false
	idx := -1
	for i, s := range cfg.Symbols {
		if strings.EqualFold(s, target) {
			found = true
			idx = i
			break
		}
	}
	fmt.Printf("watchlist mode=%s total=%d\n", cfg.Watchlist.Mode, len(cfg.Symbols))
	if found {
		fmt.Printf("%s: YES at index %d\n", target, idx)
		chunk := idx / 80
		end := (chunk+1)*80 - 1
		if end >= len(cfg.Symbols) {
			end = len(cfg.Symbols) - 1
		}
		fmt.Printf("WS chunk conn#%d (list indices %d-%d)\n", chunk, chunk*80, end)
	} else {
		fmt.Printf("%s: NOT in watchlist\n", target)
	}
	all, err := client.LowestVolumeUSDTPerpetuals(300)
	if err == nil {
		for i, s := range all {
			if s == target {
				fmt.Printf("volume rank: %d (0=lowest 24h quote vol among perps)\n", i)
				break
			}
		}
	}
}
