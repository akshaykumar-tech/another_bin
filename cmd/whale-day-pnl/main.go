package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
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

func main() {
	cfgPath := flag.String("config", "config/whale.yaml", "")
	date := flag.String("date", "2026-06-03", "UTC day YYYY-MM-DD")
	symbolsFlag := flag.String("symbols", "", "comma-separated (required)")
	maxTrades := flag.Int("max-trades", 200000, "skip if more aggTrades")
	useAPI := flag.Bool("use-api", false, "fetch aggTrades from Binance API (today / no Vision ZIP)")
	reverse := flag.Bool("reverse", false, "reverse_trade: signal BUY → trade SELL (same burst logic)")
	flag.Parse()

	if strings.TrimSpace(*symbolsFlag) == "" {
		log.Fatal("use --symbols SYM1,SYM2,...")
	}

	base, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	paper := base
	paper.BacktestEntryDelayMs = 0
	paper.BacktestExitDelayMs = 0
	paper.BacktestEntrySlippageBps = 0
	paper.BacktestExitSlippageBps = 0

	real := base
	if *reverse {
		paper.ReverseTrade = true
		real.ReverseTrade = true
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
	src := "vision"
	if *useAPI {
		src = "api"
	}
	fmt.Printf("=== %s | early_capture_all=%v | reverse_trade=%v | %d symbols | source=%s ===\n\n",
		*date, base.Burst.EarlyCaptureAll, *reverse, len(symbols), src)
	fmt.Printf("%-14s %8s %8s %10s %10s %8s\n", "symbol", "entries", "signals", "PnL paper", "PnL real", "Δ USDT")
	fmt.Println(strings.Repeat("-", 62))

	var totPaper, totReal float64
	var totEnt int

	for i, sym := range symbols {
		if *useAPI && i > 0 {
			time.Sleep(400 * time.Millisecond)
		}
		trades, err := loadAggTrades(sym, *date, client, *useAPI)
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

		sp := whale.RunBurstBacktest(paper, nil, sym, trades, false)
		sr := whale.RunBurstBacktest(real, nil, sym, trades, false)
		totPaper += sp.TotalPnLUSDT
		totReal += sr.TotalPnLUSDT
		totEnt += sp.Entries
		fmt.Printf("%-14s %8d %8d %10.2f %10.2f %8.2f\n",
			sym, sp.Entries, sp.Signals, sp.TotalPnLUSDT, sr.TotalPnLUSDT, sr.TotalPnLUSDT-sp.TotalPnLUSDT)
	}

	fmt.Println(strings.Repeat("-", 62))
	fmt.Printf("%-14s %8d %8s %10.2f %10.2f %8.2f\n", "TOTAL", totEnt, "", totPaper, totReal, totReal-totPaper)
	fmt.Printf("\npaper  = signal tick entry, no slip (ideal)\n")
	fmt.Printf("real   = entry %dms + exit %dms + slip %.0f/%.0f bps\n",
		real.BacktestEntryDelayMs, real.BacktestExitDelayMs,
		real.BacktestEntrySlippageBps, real.BacktestExitSlippageBps)
}

func loadAggTrades(symbol, date string, client *binance.FuturesClient, useAPI bool) ([]binance.AggTrade, error) {
	if !useAPI {
		tr, err := loadVisionAgg(symbol, date)
		if err == nil {
			return tr, nil
		}
	}
	if client == nil {
		return nil, fmt.Errorf("vision missing and no API client")
	}
	start, err := time.ParseInLocation("2006-01-02", date, time.UTC)
	if err != nil {
		return nil, err
	}
	end := start.Add(24 * time.Hour)
	now := time.Now().UTC()
	if end.After(now) {
		end = now
	}
	return client.FetchAggTradesRange(symbol, start, end)
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
