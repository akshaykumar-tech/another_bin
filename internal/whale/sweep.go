package whale

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"crypto_announcements_go/internal/binance"
)

type SweepResult struct {
	Name    string
	Score   float64
	Summary BacktestSummary
}

func RunFullSweep(ctx context.Context, base Config, client *binance.FuturesClient, start, end time.Time) ([]SweepResult, Config, error) {
	perps := client.USDTPerpetualSymbols()
	variants := sweepVariants(base)

	type group struct {
		key     string
		symbols []string
		vars    []sweepVariant
	}
	groups := map[string]*group{}

	for _, v := range variants {
		c := v.cfg
		if err := c.ResolveWatchlist(client, perps); err != nil {
			return nil, base, err
		}
		key := fmt.Sprintf("%s:%d:%d", c.Watchlist.Mode, c.Watchlist.Skip, c.Watchlist.Size)
		g, ok := groups[key]
		if !ok {
			g = &group{key: key, symbols: append([]string(nil), c.Symbols...)}
			groups[key] = g
		}
		g.vars = append(g.vars, v)
	}

	var results []SweepResult
	for _, g := range groups {
		loadCfg := base
		loadCfg.Symbols = g.symbols
		log.Printf("[sweep] loading %d symbols [%s]...", len(g.symbols), g.key)
		ds, err := loadMarketDataset(ctx, loadCfg, client, start, end)
		if err != nil {
			return nil, base, err
		}
		for _, v := range g.vars {
			c := v.cfg
			c.Symbols = g.symbols
			sum := RunBacktestOnDataset(c, client, ds, false)
			score := sum.TotalPnLUSDT
			if sum.Entries == 0 {
				score = -1000
			}
			results = append(results, SweepResult{Name: v.name, Score: score, Summary: sum})
			log.Printf("[sweep] %-20s sig=%3d ent=%2d pnl=%+7.2f sl=%d tp1=%d tp2=%d",
				v.name, sum.Signals, sum.Entries, sum.TotalPnLUSDT, sum.ExitsSL, sum.ExitsTP1, sum.ExitsTP2)
		}
		time.Sleep(time.Second)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Summary.TotalPnLUSDT > results[j].Summary.TotalPnLUSDT
	})

	best := pickBestConfig(base, results, variants)
	return results, best, nil
}

type sweepVariant struct {
	name string
	cfg  Config
}

func sweepVariants(base Config) []sweepVariant {
	mk := func(name string, fn func(*Config)) sweepVariant {
		c := base
		fn(&c)
		return sweepVariant{name: name, cfg: c}
	}
	return []sweepVariant{
		mk("f01_default_5pct", func(c *Config) {}),
		mk("f02_sec4_early15", func(c *Config) {
			c.Flash.MinSecMovePct = 4
			c.Flash.EarlySecMovePct = 1.5
			c.Flash.MinFastMovePct = 0.6
		}),
		mk("f03_sec6_strict", func(c *Config) {
			c.Flash.MinSecMovePct = 6
			c.Flash.MinSecNotionalUSDT = 50_000
		}),
		mk("f04_low_notional", func(c *Config) {
			c.Flash.MinSecNotionalUSDT = 15_000
			c.Flash.MinFastNotionalUSDT = 4_000
		}),
		mk("f05_tp_wide", func(c *Config) {
			c.Risk.TakeProfitPercent1 = 4
			c.Risk.TakeProfitPercent2 = 8
			c.Risk.StopLossPercent = 2
		}),
	}
}

func pickBestConfig(base Config, results []SweepResult, variants []sweepVariant) Config {
	if len(results) == 0 {
		return base
	}
	var pick *SweepResult
	for i := range results {
		r := &results[i]
		if r.Summary.Entries == 0 {
			continue
		}
		if pick == nil || r.Summary.TotalPnLUSDT > pick.Summary.TotalPnLUSDT {
			pick = r
		}
	}
	if pick == nil {
		pick = &results[0]
	}
	for _, v := range variants {
		if v.name == pick.Name {
			return v.cfg
		}
	}
	return base
}
