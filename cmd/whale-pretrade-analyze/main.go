package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

type labeledEntry struct {
	label string // mega, loss, small_win
	sym   string
	at    time.Time
	side  string
}

type preMetrics struct {
	label           string
	sym             string
	atIST           string
	quiet5s         float64
	quiet30s        float64
	quiet60s        float64
	range60sPct     float64
	trend60sPct     float64
	trades60s       int
	trades5s        int
	max1sMove60sPct float64
	avgGapMs60s     float64
	entrySecMove    float64
	entrySecVol     float64
	move5sAfterPct  float64
	move30sAfterPct float64
}

func main() {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	_ = client.WarmSymbolCache()

	// Known outcomes from retests (IST)
	entries := []labeledEntry{
		// mega
		{"mega", "SYSUSDT", time.Date(2026, 5, 13, 13, 30, 6, 0, ist), "SELL"},
		{"mega", "MLNUSDT", time.Date(2026, 5, 13, 13, 30, 6, 0, ist), "SELL"},
		{"mega", "AIGENSYNUSDT", time.Date(2026, 5, 14, 15, 30, 5, 0, ist), "BUY"},
		// 13 may non-mega (loss/small)
		{"other13", "SYSUSDT", time.Date(2026, 5, 13, 14, 10, 42, 0, ist), "BUY"},
		{"other13", "SYSUSDT", time.Date(2026, 5, 13, 14, 43, 59, 0, ist), "SELL"},
		{"other13", "ATAUSDT", time.Date(2026, 5, 13, 13, 38, 5, 0, ist), "BUY"},
		{"other13", "PHBUSDT", time.Date(2026, 5, 13, 13, 29, 0, 0, ist), "SELL"},
		// 15 may normal day
		{"normal15", "SYSUSDT", time.Date(2026, 5, 15, 12, 16, 24, 0, ist), "SELL"},
		{"normal15", "SYSUSDT", time.Date(2026, 5, 15, 13, 47, 1, 0, ist), "BUY"},
		{"normal15", "MLNUSDT", time.Date(2026, 5, 15, 11, 18, 56, 0, ist), "BUY"},
		{"normal15", "MLNUSDT", time.Date(2026, 5, 15, 12, 37, 7, 0, ist), "BUY"},
		{"normal15", "PHBUSDT", time.Date(2026, 5, 15, 14, 39, 30, 0, ist), "SELL"},
	}

	var all []preMetrics
	for _, e := range entries {
		m, err := analyzeEntry(client, e)
		if err != nil {
			log.Printf("skip %s %s: %v", e.sym, e.label, err)
			continue
		}
		all = append(all, m)
	}

	// Also scan: auto-detect all burst entries on 13/15 May for SYS+MLN+ATA+PHB
	cfg, _ := whale.LoadConfig("config/whale.yaml")
	for _, day := range []struct {
		date  time.Time
		label string
	}{
		{time.Date(2026, 5, 13, 0, 0, 0, 0, ist), "auto13"},
		{time.Date(2026, 5, 15, 0, 0, 0, 0, ist), "auto15"},
	} {
		start := time.Date(day.date.Year(), day.date.Month(), day.date.Day(), 11, 0, 0, 0, ist)
		end := time.Date(day.date.Year(), day.date.Month(), day.date.Day(), 17, 0, 0, 0, ist)
		for _, sym := range []string{"SYSUSDT", "MLNUSDT", "ATAUSDT", "PHBUSDT"} {
			trades, err := client.FetchAggTradesRange(sym, start.UTC(), end.UTC())
			if err != nil {
				continue
			}
			det := whale.NewBurstDetector(cfg.Burst)
			for _, tr := range trades {
				sig := det.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
				if sig == nil {
					continue
				}
				lbl := day.label
				if day.label == "auto13" && tr.Time.In(ist).Format("15:04") == "13:30" {
					lbl = "mega?"
				}
				pm, err := preFromTicks(sym, lbl, tr.Time, string(sig.Side), trades)
				if err == nil {
					all = append(all, pm)
				}
			}
		}
	}

	groups := map[string][]preMetrics{}
	for _, m := range all {
		groups[m.label] = append(groups[m.label], m)
	}

	fmt.Println("=== PRE-TRADE METRICS (before entry) ===")
	headers := []string{"label", "n", "quiet60s$", "quiet5s$", "range60s%", "trend60s%", "trades60s", "max1s60s%", "secVol$", "move30sAfter%"}
	for _, gname := range []string{"mega", "other13", "normal15", "auto13", "auto15"} {
		g := groups[gname]
		if len(g) == 0 {
			continue
		}
		fmt.Printf("## %s (n=%d)\n", gname, len(g))
		fmt.Printf("| %s |\n", joinRow(headers, "|"))
		fmt.Printf("|%s|\n", dashRow(len(headers)))
		fmt.Printf("| %s |\n", joinRow([]string{
			gname, fmt.Sprintf("%d", len(g)),
			avgF(g, func(m preMetrics) float64 { return m.quiet60s }),
			avgF(g, func(m preMetrics) float64 { return m.quiet5s }),
			avgF(g, func(m preMetrics) float64 { return m.range60sPct }),
			avgF(g, func(m preMetrics) float64 { return m.trend60sPct }),
			avgF(g, func(m preMetrics) float64 { return float64(m.trades60s) }),
			avgF(g, func(m preMetrics) float64 { return m.max1sMove60sPct }),
			avgF(g, func(m preMetrics) float64 { return m.entrySecVol }),
			avgF(g, func(m preMetrics) float64 { return m.move30sAfterPct }),
		}, "|"))
		fmt.Println()
	}

	fmt.Println("=== PER ENTRY (labeled) ===")
	for _, m := range all {
		if m.label == "auto13" || m.label == "auto15" || m.label == "mega?" {
			continue
		}
		fmt.Printf("%-10s %-12s %s  quiet60=$%.0f range60=%.2f%% trades60=%d max1s60=%.2f%% secVol=$%.0f → 30sAfter=%.1f%%\n",
			m.label, m.sym, m.atIST, m.quiet60s, m.range60sPct, m.trades60s, m.max1sMove60sPct, m.entrySecVol, m.move30sAfterPct)
	}

	fmt.Println("\n=== SUGGESTED FILTERS (mega vs normal15 avg) ===")
	mg, n15 := groups["mega"], groups["normal15"]
	if len(mg) > 0 && len(n15) > 0 {
		printFilterHints(mg, n15)
	}
}

func analyzeEntry(client *binance.FuturesClient, e labeledEntry) (preMetrics, error) {
	start := e.at.Add(-2 * time.Minute).UTC()
	end := e.at.Add(2 * time.Minute).UTC()
	trades, err := client.FetchAggTradesRange(e.sym, start, end)
	if err != nil {
		return preMetrics{}, err
	}
	return preFromTicks(e.sym, e.label, e.at.UTC(), e.side, trades)
}

func preFromTicks(sym, label string, at time.Time, side string, trades []binance.AggTrade) (preMetrics, error) {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	t0 := at
	var before []binance.AggTrade
	for _, tr := range trades {
		if !tr.Time.After(t0) {
			before = append(before, tr)
		}
	}
	if len(before) < 5 {
		return preMetrics{}, fmt.Errorf("insufficient ticks")
	}

	m := preMetrics{
		label: label, sym: sym, atIST: t0.In(ist).Format("15:04:05"),
		quiet5s:  notionalWindow(before, t0.Add(-5*time.Second), t0),
		quiet30s: notionalWindow(before, t0.Add(-30*time.Second), t0),
		quiet60s: notionalWindow(before, t0.Add(-60*time.Second), t0),
		trades60s: countWindow(before, t0.Add(-60*time.Second), t0),
		trades5s:  countWindow(before, t0.Add(-5*time.Second), t0),
	}
	m.range60sPct = priceRangePct(before, t0.Add(-60*time.Second), t0)
	m.trend60sPct = priceTrendPct(before, t0.Add(-60*time.Second), t0)
	m.max1sMove60sPct = max1sMove(before, t0.Add(-60*time.Second), t0)
	m.avgGapMs60s = avgGapMs(before, t0.Add(-60*time.Second), t0)
	m.move5sAfterPct = absMoveAfter(trades, t0, 5*time.Second)
	m.move30sAfterPct = absMoveAfter(trades, t0, 30*time.Second)

	// entry 1s leg
	m.entrySecVol = notionalWindow(before, t0.Add(-time.Second), t0)
	if p0 := priceAt(before, t0.Add(-time.Second)); p0 > 0 {
		p1 := before[len(before)-1].Price
		m.entrySecMove = math.Abs((p1 - p0) / p0 * 100)
	}
	_ = side
	return m, nil
}

func notionalWindow(trades []binance.AggTrade, from, to time.Time) float64 {
	var s float64
	for _, t := range trades {
		if !t.Time.Before(from) && !t.Time.After(to) {
			s += t.Price * t.Quantity
		}
	}
	return s
}

func countWindow(trades []binance.AggTrade, from, to time.Time) int {
	n := 0
	for _, t := range trades {
		if !t.Time.Before(from) && !t.Time.After(to) {
			n++
		}
	}
	return n
}

func priceAt(trades []binance.AggTrade, at time.Time) float64 {
	var p float64
	for _, t := range trades {
		if t.Time.After(at) {
			break
		}
		p = t.Price
	}
	return p
}

func priceRangePct(trades []binance.AggTrade, from, to time.Time) float64 {
	hi, lo := 0.0, math.MaxFloat64
	for _, t := range trades {
		if t.Time.Before(from) || t.Time.After(to) {
			continue
		}
		if t.Price > hi {
			hi = t.Price
		}
		if t.Price < lo {
			lo = t.Price
		}
	}
	if lo <= 0 || hi <= 0 {
		return 0
	}
	mid := (hi + lo) / 2
	return (hi - lo) / mid * 100
}

func priceTrendPct(trades []binance.AggTrade, from, to time.Time) float64 {
	p0 := priceAt(trades, from)
	p1 := priceAt(trades, to)
	if p0 <= 0 {
		return 0
	}
	return (p1 - p0) / p0 * 100
}

func max1sMove(trades []binance.AggTrade, from, to time.Time) float64 {
	var best float64
	for i := 0; i < len(trades); i++ {
		if trades[i].Time.Before(from) {
			continue
		}
		tEnd := trades[i].Time.Add(time.Second)
		p0 := trades[i].Price
		if p0 <= 0 {
			continue
		}
		for j := i; j < len(trades) && !trades[j].Time.After(tEnd); j++ {
			pct := math.Abs((trades[j].Price - p0) / p0 * 100)
			if pct > best {
				best = pct
			}
		}
	}
	return best
}

func avgGapMs(trades []binance.AggTrade, from, to time.Time) float64 {
	var gaps []float64
	var prev time.Time
	for _, t := range trades {
		if t.Time.Before(from) || t.Time.After(to) {
			continue
		}
		if !prev.IsZero() {
			gaps = append(gaps, float64(t.Time.Sub(prev).Milliseconds()))
		}
		prev = t.Time
	}
	if len(gaps) == 0 {
		return 0
	}
	sort.Float64s(gaps)
	return gaps[len(gaps)/2]
}

func absMoveAfter(trades []binance.AggTrade, from time.Time, dur time.Duration) float64 {
	p0 := 0.0
	for _, t := range trades {
		if !t.Time.Before(from) {
			p0 = t.Price
			break
		}
	}
	if p0 <= 0 {
		return 0
	}
	end := from.Add(dur)
	var p1 float64
	for _, t := range trades {
		if t.Time.Before(from) {
			continue
		}
		if t.Time.After(end) {
			break
		}
		p1 = t.Price
	}
	if p1 <= 0 {
		return 0
	}
	return math.Abs((p1 - p0) / p0 * 100)
}

func avgF(g []preMetrics, fn func(preMetrics) float64) string {
	if len(g) == 0 {
		return "-"
	}
	var s float64
	for _, m := range g {
		s += fn(m)
	}
	return fmt.Sprintf("%.0f", s/float64(len(g)))
}

func joinRow(cols []string, sep string) string {
	out := cols[0]
	for i := 1; i < len(cols); i++ {
		out += sep + cols[i]
	}
	return out
}

func dashRow(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		s += "---|"
	}
	return s
}

func printFilterHints(mega, normal []preMetrics) {
	type rule struct {
		name string
		mega float64
		norm float64
	}
	rules := []rule{
		{"quiet60s $", avg(mega, func(m preMetrics) float64 { return m.quiet60s }), avg(normal, func(m preMetrics) float64 { return m.quiet60s })},
		{"quiet5s $", avg(mega, func(m preMetrics) float64 { return m.quiet5s }), avg(normal, func(m preMetrics) float64 { return m.quiet5s })},
		{"range60s %", avg(mega, func(m preMetrics) float64 { return m.range60sPct }), avg(normal, func(m preMetrics) float64 { return m.range60sPct })},
		{"trend60s %", avg(mega, func(m preMetrics) float64 { return math.Abs(m.trend60sPct) }), avg(normal, func(m preMetrics) float64 { return math.Abs(m.trend60sPct) })},
		{"trades60s", avg(mega, func(m preMetrics) float64 { return float64(m.trades60s) }), avg(normal, func(m preMetrics) float64 { return float64(m.trades60s) })},
		{"max1s in prior 60s %", avg(mega, func(m preMetrics) float64 { return m.max1sMove60sPct }), avg(normal, func(m preMetrics) float64 { return m.max1sMove60sPct })},
		{"avg gap ms 60s", avg(mega, func(m preMetrics) float64 { return m.avgGapMs60s }), avg(normal, func(m preMetrics) float64 { return m.avgGapMs60s })},
	}
	for _, r := range rules {
		dir := "mega LOWER" 
		if r.mega > r.norm {
			dir = "mega HIGHER"
		}
		fmt.Printf("  %-22s  mega=%.1f  normal15=%.1f  → %s\n", r.name, r.mega, r.norm, dir)
	}
}

func avg(g []preMetrics, fn func(preMetrics) float64) float64 {
	var s float64
	for _, m := range g {
		s += fn(m)
	}
	return s / float64(len(g))
}
