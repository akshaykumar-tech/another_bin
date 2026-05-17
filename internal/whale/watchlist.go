package whale

import (
	"fmt"
	"log"
	"strings"

	"crypto_announcements_go/internal/binance"
)

func resolveWatchlist(cfg *Config, client *binance.FuturesClient, perps []string) error {
	mode := cfg.Watchlist.Mode
	switch mode {
	case "all":
		cfg.Symbols = perps
		log.Printf("[whale] watchlist: all %d USDT perpetuals", len(cfg.Symbols))
	case "custom":
		if len(cfg.Symbols) == 0 {
			cfg.Symbols = perps
		}
		log.Printf("[whale] watchlist: custom %d symbols", len(cfg.Symbols))
	case "lowest_volume":
		n := cfg.Watchlist.Size
		if n <= 0 {
			n = 100
		}
		syms, err := client.LowestVolumeUSDTPerpetuals(n)
		if err != nil {
			return fmt.Errorf("lowest_volume watchlist: %w", err)
		}
		cfg.Symbols = syms
		log.Printf("[whale] watchlist: %d lowest-volume USDT perpetuals", len(syms))
	case "mid_volume":
		skip := cfg.Watchlist.Skip
		n := cfg.Watchlist.Size
		if n <= 0 {
			n = 60
		}
		syms, err := client.MidVolumeUSDTPerpetuals(skip, n)
		if err != nil {
			return fmt.Errorf("mid_volume watchlist: %w", err)
		}
		cfg.Symbols = syms
		log.Printf("[whale] watchlist: %d mid-volume USDT perpetuals (skipped %d thinnest)", len(syms), skip)
	default:
		return fmt.Errorf("unknown watchlist mode %q", mode)
	}

	mergeExtraSymbols(cfg, perps)
	return nil
}

func mergeExtraSymbols(cfg *Config, perps []string) {
	extras := cfg.Watchlist.ExtraSymbols
	if len(extras) == 0 {
		return
	}
	perpSet := make(map[string]struct{}, len(perps))
	for _, s := range perps {
		perpSet[strings.ToUpper(s)] = struct{}{}
	}
	have := make(map[string]struct{}, len(cfg.Symbols))
	for _, s := range cfg.Symbols {
		have[strings.ToUpper(s)] = struct{}{}
	}
	var added []string
	for _, sym := range extras {
		sym = strings.ToUpper(strings.TrimSpace(sym))
		if sym == "" {
			continue
		}
		if _, ok := perpSet[sym]; !ok {
			log.Printf("[whale] watchlist: skip extra %s (not a USDT perpetual)", sym)
			continue
		}
		if _, ok := have[sym]; ok {
			continue
		}
		cfg.Symbols = append(cfg.Symbols, sym)
		have[sym] = struct{}{}
		added = append(added, sym)
	}
	if len(added) > 0 {
		log.Printf("[whale] watchlist: +%d extra symbols: %s", len(added), strings.Join(added, ", "))
	}
}
