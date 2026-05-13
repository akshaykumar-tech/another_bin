package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"crypto_announcements_go/internal/model"

	"github.com/go-resty/resty/v2"
)

type FuturesClient struct {
	base     string
	apiKey   string
	secret   string
	http     *resty.Client
	ruleMap  map[string]bool
	lotRules map[string]model.FuturesLotRules
}

func NewFuturesClient(base, apiKey, secret string) *FuturesClient {
	return &FuturesClient{
		base:     strings.TrimRight(base, "/"),
		apiKey:   apiKey,
		secret:   secret,
		http:     resty.New().SetTimeout(12 * time.Second).SetRetryCount(1),
		ruleMap:  make(map[string]bool),
		lotRules: make(map[string]model.FuturesLotRules),
	}
}

func (c *FuturesClient) Configured() bool {
	return strings.TrimSpace(c.apiKey) != "" && strings.TrimSpace(c.secret) != ""
}

// WarmSymbolCache loads tradable flags and PRICE_FILTER tick sizes (for stop orders).
func (c *FuturesClient) WarmSymbolCache() error {
	var payload struct {
		Symbols []struct {
			Symbol           string `json:"symbol"`
			Status           string `json:"status"`
			PricePrecision   int    `json:"pricePrecision"`
			QuantityPrecision int   `json:"quantityPrecision"`
			Filters          []struct {
				FilterType string `json:"filterType"`
				TickSize   string `json:"tickSize"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	resp, err := c.http.R().SetResult(&payload).Get(c.base + "/fapi/v1/exchangeInfo")
	if err != nil {
		return err
	}
	if resp.StatusCode() >= 300 {
		return fmt.Errorf("exchangeInfo status=%d", resp.StatusCode())
	}
	for _, s := range payload.Symbols {
		sym := strings.ToUpper(s.Symbol)
		c.ruleMap[sym] = s.Status == "TRADING"
		tick := "0.01"
		for _, f := range s.Filters {
			if f.FilterType == "PRICE_FILTER" && strings.TrimSpace(f.TickSize) != "" {
				tick = strings.TrimSpace(f.TickSize)
				break
			}
		}
		c.lotRules[sym] = model.FuturesLotRules{
			PriceTickSize:  tick,
			PricePrecision: s.PricePrecision,
		}
	}
	return nil
}

func (c *FuturesClient) SymbolTradable(symbol string) bool {
	return c.ruleMap[strings.ToUpper(symbol)]
}

func (c *FuturesClient) LotRules(symbol string) (model.FuturesLotRules, error) {
	sym := strings.ToUpper(symbol)
	r, ok := c.lotRules[sym]
	if !ok {
		return r, fmt.Errorf("unknown or inactive symbol %s", sym)
	}
	return r, nil
}

func (c *FuturesClient) MarkPrice(symbol string) (float64, error) {
	sym := strings.ToUpper(symbol)
	var out struct {
		MarkPrice string `json:"markPrice"`
	}
	resp, err := c.http.R().SetQueryParam("symbol", sym).SetResult(&out).Get(c.base + "/fapi/v1/premiumIndex")
	if err != nil {
		return 0, err
	}
	if resp.StatusCode() >= 300 {
		return 0, fmt.Errorf("premiumIndex status=%d", resp.StatusCode())
	}
	var mp float64
	_, err = fmt.Sscanf(strings.TrimSpace(out.MarkPrice), "%f", &mp)
	if err != nil || mp <= 0 {
		return 0, fmt.Errorf("invalid markPrice %q", out.MarkPrice)
	}
	return mp, nil
}

// StopMarketCloseFull places a STOP_MARKET that closes the whole position at mark (Rails: stop_market_close_full).
func (c *FuturesClient) StopMarketCloseFull(symbol, side, stopPriceStr, workingType string) (map[string]any, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("binance futures client not configured")
	}
	form := url.Values{}
	form.Set("symbol", strings.ToUpper(symbol))
	form.Set("side", strings.ToUpper(side))
	form.Set("type", "STOP_MARKET")
	form.Set("stopPrice", stopPriceStr)
	form.Set("closePosition", "true")
	form.Set("workingType", workingType)
	return c.signedPostOrder(form)
}

func (c *FuturesClient) signedPostOrder(form url.Values) (map[string]any, error) {
	body, status, err := c.signedPost("/fapi/v1/order", form)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if status >= 300 {
		return m, fmt.Errorf("binance HTTP %d: %s", status, string(body))
	}
	if code, ok := m["code"].(float64); ok && code != 0 {
		msg, _ := m["msg"].(string)
		return m, fmt.Errorf("binance error %v: %s", code, msg)
	}
	return m, nil
}

func (c *FuturesClient) signedPost(path string, form url.Values) ([]byte, int, error) {
	if !c.Configured() {
		return nil, 0, fmt.Errorf("missing API credentials")
	}
	ts := fmt.Sprintf("%d", time.Now().UnixMilli())
	form.Set("timestamp", ts)
	form.Set("recvWindow", "5000")

	keys := make([]string, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+form.Get(k))
	}
	query := strings.Join(parts, "&")
	mac := hmac.New(sha256.New, []byte(c.secret))
	_, _ = mac.Write([]byte(query))
	sig := hex.EncodeToString(mac.Sum(nil))
	payload := query + "&signature=" + sig

	resp, err := c.http.R().
		SetHeader("X-MBX-APIKEY", c.apiKey).
		SetHeader("Content-Type", "application/x-www-form-urlencoded").
		SetBody(payload).
		Post(c.base + path)
	if err != nil {
		return nil, 0, err
	}
	return resp.Body(), resp.StatusCode(), nil
}

func (c *FuturesClient) RecentMovePercent(symbol string, lookbackSec int) (float64, error) {
	end := time.Now().UnixMilli()
	start := end - int64(lookbackSec*1000)
	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	q.Set("startTime", fmt.Sprintf("%d", start))
	q.Set("endTime", fmt.Sprintf("%d", end))
	q.Set("limit", "1000")
	var rows []struct {
		Price string `json:"p"`
	}
	resp, err := c.http.R().SetQueryString(q.Encode()).SetResult(&rows).Get(c.base + "/fapi/v1/aggTrades")
	if err != nil {
		return 0, err
	}
	if resp.StatusCode() >= 300 {
		return 0, fmt.Errorf("aggTrades status=%d", resp.StatusCode())
	}
	if len(rows) < 2 {
		return 0, nil
	}
	var first, last float64
	fmt.Sscanf(rows[0].Price, "%f", &first)
	fmt.Sscanf(rows[len(rows)-1].Price, "%f", &last)
	if first <= 0 || last <= 0 {
		return 0, nil
	}
	return ((last - first) / first) * 100, nil
}

// MaxMoveInWindow checks if price ever moved significantly within the lookback window
// Returns the maximum upside and downside moves that occurred, even if corrected
func (c *FuturesClient) MaxMoveInWindow(symbol string, lookbackSec int) (maxUp, maxDown float64, err error) {
	end := time.Now().UnixMilli()
	start := end - int64(lookbackSec*1000)
	q := url.Values{}
	q.Set("symbol", strings.ToUpper(symbol))
	q.Set("interval", "1s")
	q.Set("startTime", fmt.Sprintf("%d", start))
	q.Set("endTime", fmt.Sprintf("%d", end))
	q.Set("limit", "1000")

	var klines [][]interface{}
	resp, err := c.http.R().SetQueryString(q.Encode()).SetResult(&klines).Get(c.base + "/fapi/v1/klines")
	if err != nil {
		return 0, 0, err
	}
	if resp.StatusCode() >= 300 {
		return 0, 0, fmt.Errorf("klines status=%d", resp.StatusCode())
	}
	if len(klines) == 0 {
		return 0, 0, nil
	}

	var openPrice float64
	if len(klines) > 0 && len(klines[0]) > 1 {
		fmt.Sscanf(klines[0][1].(string), "%f", &openPrice)
	}
	if openPrice <= 0 {
		return 0, 0, nil
	}

	var maxHigh, minLow float64
	for _, kline := range klines {
		if len(kline) < 3 {
			continue
		}
		var high, low float64
		fmt.Sscanf(kline[2].(string), "%f", &high)
		fmt.Sscanf(kline[3].(string), "%f", &low)

		if high > maxHigh || maxHigh == 0 {
			maxHigh = high
		}
		if low < minLow || minLow == 0 {
			minLow = low
		}
	}

	if maxHigh > 0 {
		maxUp = ((maxHigh - openPrice) / openPrice) * 100
	}
	if minLow > 0 {
		maxDown = ((openPrice - minLow) / openPrice) * 100
	}

	return maxUp, maxDown, nil
}

// MarketOrder places a MARKET order on Binance USD-M Futures.
// Uses quoteOrderQty to specify size in USDT directly — no MarkPrice HTTP call needed.
// Single HTTP round trip: POST /fapi/v1/order.
func (c *FuturesClient) MarketOrder(symbol, side string, marginUSDT float64) (map[string]any, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("binance futures client not configured (missing API key/secret)")
	}
	form := url.Values{}
	form.Set("symbol", strings.ToUpper(symbol))
	form.Set("side", strings.ToUpper(side))
	form.Set("type", "MARKET")
	form.Set("quoteOrderQty", fmt.Sprintf("%.2f", marginUSDT))
	form.Set("newOrderRespType", "RESULT")
	return c.signedPostOrder(form)
}
