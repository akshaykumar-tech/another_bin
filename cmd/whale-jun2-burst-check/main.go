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

type probeEvent struct {
	Symbol  string  `json:"symbol"`
	SecMs   int64   `json:"sec_ms"`
	NetPct  float64 `json:"net_pct"`
	TMs     int64   `json:"t_ms"`
}

func main() {
	eventsPath := flag.String("events", "/tmp/jun2_rolling_net11.json", "JSON array of probe events")
	cfgPath := flag.String("config", "config/whale.yaml", "whale config")
	skipSyms := flag.String("skip", "EPICUSDT", "comma-separated symbols to ignore")
	flag.Parse()

	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	raw, err := os.ReadFile(*eventsPath)
	if err != nil {
		log.Fatal(err)
	}
	var events []probeEvent
	if err := json.Unmarshal(raw, &events); err != nil {
		log.Fatal(err)
	}
	skip := map[string]bool{}
	for _, s := range splitComma(*skipSyms) {
		skip[s] = true
	}
	filtered := events[:0]
	for _, e := range events {
		if skip[e.Symbol] {
			continue
		}
		filtered = append(filtered, e)
	}
	events = filtered

	bySym := map[string][]probeEvent{}
	for _, e := range events {
		bySym[e.Symbol] = append(bySym[e.Symbol], e)
	}

	ist, _ := time.LoadLocation("Asia/Kolkata")
	fmt.Printf("=== burst capture probe (config=%s) ===\n\n", *cfgPath)

	var fired, blocked, noCandidate int

	for sym, evs := range bySym {
		all, err := loadVisionAgg(sym, "2026-06-02")
		if err != nil {
			fmt.Printf("## %s — load error: %v\n\n", sym, err)
			continue
		}
		// Pre-trade uses up to 3h tape; keep a tight window around probes for speed.
		var minT, maxT time.Time
		for _, ev := range evs {
			t := time.UnixMilli(ev.TMs)
			if ev.TMs == 0 {
				t = time.UnixMilli(ev.SecMs + 500)
			}
			if minT.IsZero() || t.Before(minT) {
				minT = t
			}
			if maxT.IsZero() || t.After(maxT) {
				maxT = t
			}
		}
		warm := 3*time.Hour + 2*time.Minute
		from := minT.Add(-warm)
		to := maxT.Add(2 * time.Minute)
		trades := filterTrades(all, from, to)
		fmt.Printf("## %s (%d aggTrades in probe window, %d probe seconds)\n", sym, len(trades), len(evs))

		for _, ev := range evs {
			target := time.UnixMilli(ev.TMs)
			if ev.TMs == 0 {
				target = time.UnixMilli(ev.SecMs + 500)
			}
			diag, max5, max30 := whale.DiagnoseBurstNearFast(cfg.Burst, trades, target, 2*time.Second)
			snap := whale.PreTradeAt(cfg.Burst, trades, target)

			ts := target.UTC().Format("15:04:05")
			tsIST := target.In(ist).Format("15:04:05")

			if diag.Fired {
				fired++
				fmt.Printf("  ✓ %s UTC (%s IST) rolling_net=%+.2f%% → SIGNAL %s 1s=%.2f%% vol=$%.0f | after: 5s=%.1f%% 30s=%.1f%%\n",
					ts, tsIST, ev.NetPct, diag.Side, diag.SecMove, diag.SecNotional, max5, max30)
			} else if diag.RejectReason != "" {
				blocked++
				fmt.Printf("  ✗ %s UTC (%s IST) net=%+.2f%% → blocked: %s\n", ts, tsIST, ev.NetPct, diag.RejectReason)
				fmt.Printf("      pre: q60=$%.0f q2h=$%.0f range2h=%.2f%% prior1s=%.2f%% flat=%v elevated=%v\n",
					snap.Quiet60, snap.Quiet2h, snap.Range2h, snap.Prior1s, snap.FlatMega, snap.ElevatedMega)
			} else {
				noCandidate++
				fmt.Printf("  ? %s UTC (%s IST) net=%+.2f%% → no burst candidate ±2s (move/vol too small at ticks)\n",
					ts, tsIST, ev.NetPct)
			}
		}
		fmt.Println()
	}

	fmt.Printf("SUMMARY: %d events | fired=%d blocked=%d no_candidate=%d\n", len(events), fired, blocked, noCandidate)
}

func splitComma(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(strings.ToUpper(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func filterTrades(trades []binance.AggTrade, from, to time.Time) []binance.AggTrade {
	out := make([]binance.AggTrade, 0, len(trades)/24)
	for _, tr := range trades {
		if !tr.Time.Before(from) && !tr.Time.After(to) {
			out = append(out, tr)
		}
	}
	return out
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
		if len(f.Name) > 4 && f.Name[len(f.Name)-4:] == ".csv" {
			csvName = f.Name
			break
		}
	}
	if csvName == "" {
		return nil, fmt.Errorf("no csv in zip")
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
		if err != nil {
			return nil, err
		}
		if row[0] == "agg_trade_id" {
			continue
		}
		px, _ := strconv.ParseFloat(row[1], 64)
		qty, _ := strconv.ParseFloat(row[2], 64)
		ts, _ := strconv.ParseInt(row[5], 10, 64)
		maker := row[6] == "true"
		out = append(out, binance.AggTrade{
			Price: px, Quantity: qty, Time: time.UnixMilli(ts), BuyerIsMaker: maker,
		})
	}
	return out, nil
}
