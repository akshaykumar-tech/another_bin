package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

type result struct {
	sym    string
	trades int
	sum    whale.BacktestSummary
	err    error
	skip   string
}

type probeEvent struct {
	Symbol string  `json:"symbol"`
	SecMs  int64   `json:"sec_ms"`
	NetPct float64 `json:"net_pct"`
	TMs    int64   `json:"t_ms"`
}

func main() {
	cfgPath := flag.String("config", "config/whale.yaml", "")
	date := flag.String("date", "2026-06-02", "UTC day YYYY-MM-DD")
	eventsPath := flag.String("events", "/tmp/jun2_rolling_net11.json", "known ≥6%% rolling seconds")
	skip := flag.String("skip", "EPICUSDT,PORTALUSDT", "comma-separated; slow/heavy symbols")
	maxSec := flag.Int("max-sec-per-symbol", 90, "skip symbol if download+backtest exceeds this")
	maxTrades := flag.Int("max-trades", 150000, "skip symbol if aggTrades count exceeds this")
	flag.Parse()

	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	raw, _ := os.ReadFile(*eventsPath)
	var probes []probeEvent
	_ = json.Unmarshal(raw, &probes)
	skipSet := map[string]bool{}
	for _, s := range strings.Split(*skip, ",") {
		s = strings.TrimSpace(strings.ToUpper(s))
		if s != "" {
			skipSet[s] = true
		}
	}

	// Symbols that had ≥6% wick/net activity on 2 Jun (from scan report).
	symbols := []string{
		"EVAAUSDT", "JCTUSDT", "KOMAUSDT", "NOMUSDT", "PUMPBTCUSDT",
		"PORTALUSDT", "TACUSDT", "TAKEUSDT",
	}
	if *skip != "" {
		var filt []string
		for _, s := range symbols {
			if !skipSet[s] {
				filt = append(filt, s)
			}
		}
		symbols = filt
	}

	ist, _ := time.LoadLocation("Asia/Kolkata")
	fmt.Printf("=== %s burst backtest | early_capture_all=%v ===\n", *date, cfg.Burst.EarlyCaptureAll)
	fmt.Printf("realism: entry_delay=%dms exit_delay=%dms entry_slip=%.0fbps exit_slip=%.0fbps\n\n",
		cfg.BacktestEntryDelayMs, cfg.BacktestExitDelayMs, cfg.BacktestEntrySlippageBps, cfg.BacktestExitSlippageBps)

	var totalPnL float64
	var totalSignals, totalEntries int
	perSym := make(map[string]whale.BacktestSummary)

	for _, sym := range symbols {
		ch := make(chan result, 1)
		go func(s string) {
			trades, err := loadVisionAgg(s, *date)
			if err != nil {
				ch <- result{sym: s, err: err}
				return
			}
			if *maxTrades > 0 && len(trades) > *maxTrades {
				ch <- result{sym: s, skip: fmt.Sprintf("aggTrades %d > max %d", len(trades), *maxTrades)}
				return
			}
			sum := whale.RunBurstBacktest(cfg, nil, s, trades, false)
			ch <- result{sym: s, trades: len(trades), sum: sum}
		}(sym)
		var r result
		select {
		case r = <-ch:
		case <-time.After(time.Duration(*maxSec) * time.Second):
			fmt.Printf("%s: SKIP (>%ds)\n", sym, *maxSec)
			continue
		}
		if r.skip != "" {
			fmt.Printf("%s: SKIP (%s)\n", sym, r.skip)
			continue
		}
		if r.err != nil {
			fmt.Printf("%s: load err %v\n", sym, r.err)
			continue
		}
		sum := r.sum
		tradesN := r.trades
		perSym[sym] = sum
		totalPnL += sum.TotalPnLUSDT
		totalSignals += sum.Signals
		totalEntries += sum.Entries
		fmt.Printf("%s: trades=%d signals=%d entries=%d PnL=%+.2f USDT (SL=%d trail=%d timeout=%d)\n",
			sym, tradesN, sum.Signals, sum.Entries, sum.TotalPnLUSDT,
			sum.ExitsSL, sum.ExitsTP2+sum.ExitsTP1, sum.ExitsTimeout)
		_ = tradesN
	}

	fmt.Printf("\n--- ACTIVE SYMBOLS TOTAL ---\n")
	fmt.Printf("symbols=%d signals=%d entries=%d PnL=%+.2f USDT\n\n", len(symbols), totalSignals, totalEntries, totalPnL)

	fmt.Printf("--- PROBE: 11 rolling net≥6%% seconds — entry within 120s? ---\n")
	captured := 0
	for _, p := range probes {
		if skipSet[p.Symbol] {
			continue
		}
		target := time.UnixMilli(p.TMs)
		if p.TMs == 0 {
			target = time.UnixMilli(p.SecMs + 500)
		}
		hit, at, move, probeSkip := probeCapture(cfg, p.Symbol, *date, target, *maxSec, *maxTrades)
		if probeSkip != "" {
			fmt.Printf("  ? %s probe SKIP (%s)\n", p.Symbol, probeSkip)
			continue
		}
		ts := target.In(ist).Format("15:04:05")
		if hit {
			captured++
			fmt.Printf("  ✓ %s %s IST net_probe=%+.2f%% → entry ~%s 1s=%.2f%%\n",
				p.Symbol, ts, p.NetPct, at.In(ist).Format("15:04:05"), move)
		} else {
			fmt.Printf("  ✗ %s %s IST net_probe=%+.2f%% → no entry ±120s\n", p.Symbol, ts, p.NetPct)
		}
	}
	asked := len(probes)
	for s := range skipSet {
		for _, p := range probes {
			if p.Symbol == s {
				asked--
			}
		}
	}
	fmt.Printf("\nCaptured %d / %d probe seconds (EPIC skipped in symbol backtest)\n", captured, asked)
}

func probeCapture(cfg whale.Config, sym, date string, target time.Time, maxSec, maxTrades int) (bool, time.Time, float64, string) {
	ch := make(chan struct {
		hit, skip       bool
		at              time.Time
		move            float64
		skipReason      string
	}, 1)
	go func() {
		trades, err := loadVisionAgg(sym, date)
		if err != nil {
			ch <- struct {
				hit, skip       bool
				at              time.Time
				move            float64
				skipReason      string
			}{skip: true, skipReason: err.Error()}
			return
		}
		if maxTrades > 0 && len(trades) > maxTrades {
			ch <- struct {
				hit, skip       bool
				at              time.Time
				move            float64
				skipReason      string
			}{skip: true, skipReason: fmt.Sprintf("aggTrades %d", len(trades))}
			return
		}
		h, a, m := findEntryNear(cfg, sym, trades, target, 120*time.Second)
		ch <- struct {
			hit, skip       bool
			at              time.Time
			move            float64
			skipReason      string
		}{hit: h, at: a, move: m}
	}()
	select {
	case r := <-ch:
		if r.skip {
			return false, time.Time{}, 0, r.skipReason
		}
		return r.hit, r.at, r.move, ""
	case <-time.After(time.Duration(maxSec) * time.Second):
		return false, time.Time{}, 0, fmt.Sprintf(">%ds", maxSec)
	}
}

func findEntryNear(cfg whale.Config, sym string, trades []binance.AggTrade, target time.Time, win time.Duration) (bool, time.Time, float64) {
	st := whale.NewBurstDetector(cfg.Burst)
	from := target.Add(-3 * time.Hour)
	for _, tr := range trades {
		if tr.Time.Before(from) {
			continue
		}
		if tr.Time.After(target.Add(win)) {
			break
		}
		sig := st.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
		if sig != nil && !tr.Time.Before(target.Add(-win)) && !tr.Time.After(target.Add(win)) {
			return true, tr.Time, sig.MovePct
		}
	}
	return false, time.Time{}, 0
}

func loadVisionAgg(symbol, date string) ([]binance.AggTrade, error) {
	url := fmt.Sprintf(
		"https://data.binance.vision/data/futures/um/daily/aggTrades/%s/%s-aggTrades-%s.zip",
		symbol, symbol, date,
	)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, err
	}
	var csvName string
	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, ".csv") {
			csvName = f.Name
			break
		}
	}
	rc, err := zr.Open(csvName)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	r := csv.NewReader(rc)
	var out []binance.AggTrade
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || row[0] == "agg_trade_id" {
			continue
		}
		px, _ := strconv.ParseFloat(row[1], 64)
		qty, _ := strconv.ParseFloat(row[2], 64)
		ts, _ := strconv.ParseInt(row[5], 10, 64)
		out = append(out, binance.AggTrade{
			Price: px, Quantity: qty, Time: time.UnixMilli(ts), BuyerIsMaker: row[6] == "true",
		})
	}
	return out, nil
}
