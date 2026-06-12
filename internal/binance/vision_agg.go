package binance

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const DefaultAggDataDir = "data/aggtrades"

// Jun2ExtremeSymbols had ≥6% 1s legs on 2026-06-02 (Vision scan).
var Jun2ExtremeSymbols = []string{
	"EVAAUSDT", "JCTUSDT", "KOMAUSDT", "NOMUSDT", "PUMPBTCUSDT",
	"PORTALUSDT", "TACUSDT", "TAKEUSDT",
}

type storedAggTrade struct {
	ID           int64   `json:"a,omitempty"`
	Price        float64 `json:"p"`
	Quantity     float64 `json:"q"`
	TimeMs       int64   `json:"t"`
	BuyerIsMaker bool    `json:"m"`
}

func AggTradeStorePath(dataDir, date, symbol string) string {
	return filepath.Join(dataDir, date, strings.ToUpper(symbol)+".json")
}

func SaveAggTrades(path string, trades []AggTrade) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	rows := make([]storedAggTrade, len(trades))
	for i, tr := range trades {
		rows[i] = storedAggTrade{
			ID: tr.ID, Price: tr.Price, Quantity: tr.Quantity,
			TimeMs: tr.Time.UnixMilli(), BuyerIsMaker: tr.BuyerIsMaker,
		}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func LoadAggTradesFile(path string) ([]AggTrade, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []storedAggTrade
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, err
	}
	out := make([]AggTrade, len(rows))
	for i, r := range rows {
		out[i] = AggTrade{
			ID: r.ID, Price: r.Price, Quantity: r.Quantity,
			Time: time.UnixMilli(r.TimeMs).UTC(), BuyerIsMaker: r.BuyerIsMaker,
		}
	}
	return out, nil
}

// FetchVisionAgg downloads one UTC day from Binance Vision daily aggTrades ZIP.
func FetchVisionAgg(symbol, date string) ([]AggTrade, error) {
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
		return nil, fmt.Errorf("vision HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return parseVisionAggZip(body)
}

func parseVisionAggZip(body []byte) ([]AggTrade, error) {
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
	if csvName == "" {
		return nil, fmt.Errorf("no csv in vision zip")
	}
	rc, err := zr.Open(csvName)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return parseVisionAggCSV(rc)
}

func parseVisionAggCSV(r io.Reader) ([]AggTrade, error) {
	// lightweight CSV parse without encoding/csv import cycle concerns
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var out []AggTrade
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "agg_trade_id") {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 7 {
			continue
		}
		id, _ := strconv.ParseInt(parts[0], 10, 64)
		px, _ := strconv.ParseFloat(parts[1], 64)
		qty, _ := strconv.ParseFloat(parts[2], 64)
		ts, _ := strconv.ParseInt(parts[5], 10, 64)
		out = append(out, AggTrade{
			ID: id, Price: px, Quantity: qty,
			Time: time.UnixMilli(ts).UTC(), BuyerIsMaker: parts[6] == "true",
		})
	}
	return out, nil
}

// LoadAggTradesDay prefers local JSON cache, then Vision, then API (if client set).
func LoadAggTradesDay(symbol, date, dataDir string, client *FuturesClient, useAPI bool) ([]AggTrade, string, error) {
	sym := strings.ToUpper(symbol)
	if dataDir == "" {
		dataDir = DefaultAggDataDir
	}
	path := AggTradeStorePath(dataDir, date, sym)
	if tr, err := LoadAggTradesFile(path); err == nil && len(tr) > 0 {
		return tr, "local", nil
	}
	if !useAPI {
		if tr, err := FetchVisionAgg(sym, date); err == nil {
			return tr, "vision", nil
		}
	}
	if client == nil {
		return nil, "", fmt.Errorf("no local cache and no API client for %s", sym)
	}
	start, err := time.ParseInLocation("2006-01-02", date, time.UTC)
	if err != nil {
		return nil, "", err
	}
	end := start.Add(24 * time.Hour)
	now := time.Now().UTC()
	if end.After(now) {
		end = now
	}
	tr, err := client.FetchAggTradesRange(sym, start, end)
	if err != nil {
		return nil, "", err
	}
	return tr, "api", nil
}

// DownloadAggTradesDay fetches Vision ZIP and writes local JSON cache.
func DownloadAggTradesDay(symbol, date, dataDir string, force bool) (int, error) {
	path := AggTradeStorePath(dataDir, date, symbol)
	if !force {
		if tr, err := LoadAggTradesFile(path); err == nil && len(tr) > 0 {
			return len(tr), nil
		}
	}
	trades, err := FetchVisionAgg(symbol, date)
	if err != nil {
		return 0, err
	}
	if err := SaveAggTrades(path, trades); err != nil {
		return 0, err
	}
	return len(trades), nil
}
