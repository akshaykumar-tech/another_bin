// Grid-search burst params on cached Vision days; compares same vs reverse direction.
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

type dayCache struct {
	date   string
	symbol string
	trades []binance.AggTrade
}

type cand struct {
	Reverse    bool
	Sides      string
	MinSec     float64
	Ratio      float64
	MinNotional float64
	NoMega     float64
	NoMegaMs   int
	Bounce     float64
	SL         float64
}

type result struct {
	cand
	Entries int
	Signals int
	PnL     float64
	PerDay  map[string]float64
}

func main() {
	log.SetFlags(0)
	cfgPath := flag.String("config", "config/whale.yaml", "")
	datesFlag := flag.String("dates", "2026-06-02,2026-06-03", "comma-separated UTC days")
	symbolsFlag := flag.String("symbols", "", "comma-separated (default: jun2 extreme 8)")
	dataDir := flag.String("data-dir", binance.DefaultAggDataDir, "")
	maxTrades := flag.Int("max-trades", 250000, "skip symbol-day above this")
	minEntries := flag.Int("min-entries", 3, "min total entries to rank")
	margin := flag.Float64("margin", 1, "sim margin USDT")
	lev := flag.Int("lev", 10, "sim leverage")
	topN := flag.Int("top", 20, "print top N configs")
	flag.Parse()

	base, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}

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

	var dates []string
	for _, d := range strings.Split(*datesFlag, ",") {
		d = strings.TrimSpace(d)
		if d != "" {
			dates = append(dates, d)
		}
	}
	if len(dates) == 0 || len(symbols) == 0 {
		log.Fatal("need dates and symbols")
	}

	var cache []dayCache
	for _, date := range dates {
		for _, sym := range symbols {
			trades, src, err := binance.LoadAggTradesDay(sym, date, *dataDir, nil, false)
			if err != nil {
				fmt.Printf("SKIP %s %s: %v\n", date, sym, err)
				continue
			}
			if *maxTrades > 0 && len(trades) > *maxTrades {
				fmt.Printf("SKIP %s %s: %d trades > max %d\n", date, sym, len(trades), *maxTrades)
				continue
			}
			if len(trades) < 100 {
				continue
			}
			cache = append(cache, dayCache{date: date, symbol: sym, trades: trades})
			_ = src
		}
	}
	if len(cache) == 0 {
		log.Fatal("no cached data loaded")
	}

	fmt.Printf("optimize | %d symbol-days | margin=%.0f lev=%dx | slip=%.0fbps delay=%d/%dms\n",
		len(cache), *margin, *lev, base.BacktestEntrySlippageBps,
		base.BacktestEntryDelayMs, base.BacktestExitDelayMs)
	fmt.Printf("dates=%v symbols=%d\n\n", dates, len(symbols))

	candidates := buildCandidates()
	var results []result
	t0 := time.Now()

	for i, c := range candidates {
		if i > 0 && i%20 == 0 {
			fmt.Printf("  ... %d/%d configs (%.0fs)\n", i, len(candidates), time.Since(t0).Seconds())
		}
		cfg := applyCand(base, c, *margin, *lev)
		r := result{cand: c, PerDay: make(map[string]float64)}
		for _, dc := range cache {
			sum := whale.RunBurstBacktest(cfg, nil, dc.symbol, dc.trades, false)
			r.Entries += sum.Entries
			r.Signals += sum.Signals
			r.PnL += sum.TotalPnLUSDT
			r.PerDay[dc.date] += sum.TotalPnLUSDT
		}
		if r.Entries >= *minEntries {
			results = append(results, r)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].PnL != results[j].PnL {
			return results[i].PnL > results[j].PnL
		}
		return results[i].Entries > results[j].Entries
	})

	limit := *topN
	if len(results) < limit {
		limit = len(results)
	}
	fmt.Printf("TOP %d (entries>=%d):\n", limit, *minEntries)
	for i := 0; i < limit; i++ {
		r := results[i]
		fmt.Printf("%2d. %+7.2f USDT  ent=%3d sig=%3d  %s\n", i+1, r.PnL, r.Entries, r.Signals, r.cand.label())
		for _, d := range dates {
			fmt.Printf("      %s: %+6.2f\n", d, r.PerDay[d])
		}
	}

	if len(results) == 0 {
		fmt.Println("\nno configs met min-entries")
		return
	}

	bestSame, bestRev := findBestDir(results, false), findBestDir(results, true)
	fmt.Println("\n=== DIRECTION SUMMARY ===")
	if bestSame != nil {
		fmt.Printf("BEST SAME:      %+7.2f USDT ent=%d  %s\n", bestSame.PnL, bestSame.Entries, bestSame.cand.label())
	} else {
		fmt.Println("BEST SAME:      (none)")
	}
	if bestRev != nil {
		fmt.Printf("BEST REVERSE:   %+7.2f USDT ent=%d  %s\n", bestRev.PnL, bestRev.Entries, bestRev.cand.label())
	} else {
		fmt.Println("BEST REVERSE:   (none)")
	}
	b := results[0]
	fmt.Printf("\nOVERALL BEST: %+7.2f USDT ent=%d\n%s\n", b.PnL, b.Entries, b.cand.yamlSnippet())
}

func buildCandidates() []cand {
	// Focused grid (~192 combos): sell-only for same-dir dumps; both sides when reversing.
	var out []cand
	for _, rev := range []bool{false, true} {
		sideOpts := []string{"sell"}
		if rev {
			sideOpts = []string{"sell", "both"}
		}
		for _, sides := range sideOpts {
			for _, minSec := range []float64{1.8, 2.0, 2.2} {
				for _, ratio := range []float64{0.70, 0.80} {
					for _, minN := range []float64{12000, 18000, 20000} {
						for _, nm := range []struct {
							pct float64
							ms  int
						}{{0, 0}, {0.25, 45000}, {0.35, 40000}} {
							for _, bounce := range []float64{0, 0.35} {
								out = append(out, cand{
									Reverse: rev, Sides: sides, MinSec: minSec, Ratio: ratio,
									MinNotional: minN, NoMega: nm.pct, NoMegaMs: nm.ms,
									Bounce: bounce, SL: 1.0,
								})
							}
						}
					}
				}
			}
		}
	}
	return out
}

func applyCand(base whale.Config, c cand, margin float64, lev int) whale.Config {
	cfg := base
	cfg.MarginUSDT = margin
	cfg.Leverage = lev
	cfg.ReverseTrade = c.Reverse
	cfg.Burst.SignalSides = c.Sides
	cfg.Burst.MinSecMovePct = c.MinSec
	cfg.Burst.MinViolentSecMovePct = c.MinSec
	cfg.Burst.MinFastSecRatio = c.Ratio
	cfg.Burst.MinSecNotionalUSDT = c.MinNotional
	cfg.Burst.MinViolentSecNotionalUSDT = c.MinNotional
	cfg.Burst.MaxEntryBouncePct = c.Bounce
	cfg.Risk.MegaStopLossPercent = c.SL
	cfg.Risk.MegaConfirmMinFavorablePct = c.NoMega
	cfg.Risk.MegaConfirmWindowMs = c.NoMegaMs
	cfg.Risk.MegaTrailActivatePct = 0.35
	cfg.Risk.MegaTrailMinHoldMs = 3000
	cfg.Risk.MegaSLSignalRatio = 0.75
	cfg.Risk.MegaSLSignalMinMovePct = 2.0
	return cfg
}

func findBestDir(results []result, reverse bool) *result {
	var best *result
	for i := range results {
		r := &results[i]
		if r.Reverse != reverse {
			continue
		}
		if best == nil || r.PnL > best.PnL {
			best = r
		}
	}
	return best
}

func (c cand) label() string {
	dir := "same"
	if c.Reverse {
		dir = "reverse"
	}
	return fmt.Sprintf("%s sides=%s sec=%.1f%% r=%.2f notional=%.0f nm=%.2f/%dms bounce=%.2f sl=%.1f",
		dir, c.Sides, c.MinSec, c.Ratio, c.MinNotional, c.NoMega, c.NoMegaMs, c.Bounce, c.SL)
}

func (c cand) yamlSnippet() string {
	rev := "false"
	if c.Reverse {
		rev = "true"
	}
	nmWin := c.NoMegaMs
	nmPct := c.NoMega
	return fmt.Sprintf(`# optimizer pick %s
burst:
  signal_sides: %s
  min_sec_move_pct: %.1f
  min_violent_sec_move_pct: %.1f
  min_fast_sec_ratio: %.2f
  min_sec_notional_usdt: %.0f
  min_violent_sec_notional_usdt: %.0f
  max_entry_bounce_pct: %.2f
risk:
  mega_stop_loss_percent: %.1f
  mega_confirm_window_ms: %d
  mega_confirm_min_favorable_pct: %.2f
reverse_trade: %s`,
		time.Now().UTC().Format(time.RFC3339), c.Sides, c.MinSec, c.MinSec, c.Ratio,
		c.MinNotional, c.MinNotional, c.Bounce, c.SL, nmWin, nmPct, rev)
}
