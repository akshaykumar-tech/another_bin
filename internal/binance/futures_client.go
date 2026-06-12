package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"crypto_announcements_go/internal/model"

	"github.com/go-resty/resty/v2"
)

type FuturesClient struct {
	base        string
	apiKey      string
	secret      string
	http        *resty.Client
	ruleMap     map[string]bool
	lotRules    map[string]model.FuturesLotRules
	maxLeverage map[string]int
}

func NewFuturesClient(base, apiKey, secret string) *FuturesClient {
	return &FuturesClient{
		base:     strings.TrimRight(base, "/"),
		apiKey:   apiKey,
		secret:   secret,
		http:     resty.New().SetTimeout(12 * time.Second).SetRetryCount(1),
		ruleMap:     make(map[string]bool),
		lotRules:    make(map[string]model.FuturesLotRules),
		maxLeverage: make(map[string]int),
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
				FilterType  string `json:"filterType"`
				TickSize    string `json:"tickSize"`
				StepSize    string `json:"stepSize"`
				MinQty      string `json:"minQty"`
				Notional    string `json:"notional"`
				MinNotional string `json:"minNotional"`
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
		stepSize := 0.0
		minQty := 0.0
		minNotional := 5.0
		for _, f := range s.Filters {
			switch f.FilterType {
			case "PRICE_FILTER":
				if strings.TrimSpace(f.TickSize) != "" {
					tick = strings.TrimSpace(f.TickSize)
				}
			case "LOT_SIZE":
				stepSize = parseFilterFloat(f.StepSize)
				minQty = parseFilterFloat(f.MinQty)
			case "MIN_NOTIONAL":
				if v := parseFilterFloat(f.Notional); v > 0 {
					minNotional = v
				}
			}
		}
		c.lotRules[sym] = model.FuturesLotRules{
			PriceTickSize:  tick,
			PricePrecision: s.PricePrecision,
			StepSize:       stepSize,
			MinQty:         minQty,
			MinNotional:    minNotional,
		}
	}
	if err := c.loadLeverageBrackets(); err != nil {
		for sym := range c.ruleMap {
			c.maxLeverage[sym] = 125
		}
	}
	return nil
}

func (c *FuturesClient) loadLeverageBrackets() error {
	if !c.Configured() {
		for sym := range c.ruleMap {
			c.maxLeverage[sym] = 125
		}
		return nil
	}
	body, status, err := c.signedGet("/fapi/v1/leverageBracket", url.Values{})
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("leverageBracket status=%d: %s", status, string(body))
	}
	var rows []struct {
		Symbol   string `json:"symbol"`
		Brackets []struct {
			InitialLeverage int `json:"initialLeverage"`
		} `json:"brackets"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return err
	}
	for _, row := range rows {
		sym := strings.ToUpper(row.Symbol)
		maxLev := 1
		for _, b := range row.Brackets {
			if b.InitialLeverage > maxLev {
				maxLev = b.InitialLeverage
			}
		}
		if maxLev > 0 {
			c.maxLeverage[sym] = maxLev
		}
	}
	return nil
}

// MaxLeverage returns exchange max initial leverage for symbol (0 if unknown).
func (c *FuturesClient) MaxLeverage(symbol string) int {
	sym := strings.ToUpper(symbol)
	if m, ok := c.maxLeverage[sym]; ok && m > 0 {
		return m
	}
	return 125
}

// EffectiveLeverage caps requested leverage by symbol max.
func (c *FuturesClient) EffectiveLeverage(symbol string, requested int) int {
	if requested <= 0 {
		requested = 1
	}
	max := c.MaxLeverage(symbol)
	if max <= 0 {
		return requested
	}
	if requested > max {
		return max
	}
	return requested
}

// SetLeverage sets account leverage for a symbol (signed).
func (c *FuturesClient) SetLeverage(symbol string, leverage int) error {
	if !c.Configured() {
		return nil
	}
	if leverage <= 0 {
		leverage = 1
	}
	form := url.Values{}
	form.Set("symbol", strings.ToUpper(symbol))
	form.Set("leverage", fmt.Sprintf("%d", leverage))
	_, status, err := c.signedPost("/fapi/v1/leverage", form)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("set leverage status=%d", status)
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
func (c *FuturesClient) AvailableUSDTBalance() (float64, error) {
	if !c.Configured() {
		return 0, fmt.Errorf("binance futures client not configured")
	}
	form := url.Values{}
	body, status, err := c.signedGet("/fapi/v2/balance", form)
	if err != nil {
		return 0, err
	}
	if status >= 300 {
		return 0, fmt.Errorf("balance status=%d: %s", status, string(body))
	}
	var rows []struct {
		Asset            string `json:"asset"`
		AvailableBalance string `json:"availableBalance"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return 0, err
	}
	for _, r := range rows {
		if strings.ToUpper(r.Asset) == "USDT" {
			var bal float64
			fmt.Sscanf(strings.TrimSpace(r.AvailableBalance), "%f", &bal)
			return bal, nil
		}
	}
	return 0, fmt.Errorf("USDT balance not found")
}

func (c *FuturesClient) signedGet(path string, form url.Values) ([]byte, int, error) {
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
	resp, err := c.http.R().SetHeader("X-MBX-APIKEY", c.apiKey).Get(c.base + path + "?" + query + "&signature=" + sig)
	if err != nil {
		return nil, 0, err
	}
	return resp.Body(), resp.StatusCode(), nil
}

func parseFilterFloat(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func floorToStep(qty, step float64) float64 {
	if step <= 0 {
		return qty
	}
	return math.Floor(qty/step) * step
}

func (c *FuturesClient) formatQty(symbol string, qty float64) (float64, error) {
	rules, err := c.LotRules(symbol)
	if err != nil {
		return qty, err
	}
	q := floorToStep(qty, rules.StepSize)
	if rules.MinQty > 0 && q < rules.MinQty {
		q = rules.MinQty
	}
	if q <= 0 {
		return 0, fmt.Errorf("quantity rounds to zero")
	}
	return q, nil
}

func (c *FuturesClient) marketOrderOpenQty(symbol, side string, qty float64) (map[string]any, error) {
	q, err := c.formatQty(symbol, qty)
	if err != nil {
		return nil, err
	}
	form := url.Values{}
	form.Set("symbol", strings.ToUpper(symbol))
	form.Set("side", strings.ToUpper(side))
	form.Set("type", "MARKET")
	form.Set("quantity", fmt.Sprintf("%.8f", q))
	form.Set("newOrderRespType", "RESULT")
	return c.signedPostOrder(form)
}

func (c *FuturesClient) MarketOrderQty(symbol, side string, qty float64) (map[string]any, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("binance futures client not configured")
	}
	q, err := c.formatQty(symbol, qty)
	if err != nil {
		return nil, err
	}
	form := url.Values{}
	form.Set("symbol", strings.ToUpper(symbol))
	form.Set("side", strings.ToUpper(side))
	form.Set("type", "MARKET")
	form.Set("quantity", fmt.Sprintf("%.8f", q))
	form.Set("reduceOnly", "true")
	form.Set("newOrderRespType", "RESULT")
	return c.signedPostOrder(form)
}

// MarketOrder opens a MARKET position using quantity derived from USDT notional (works on all USDT-M symbols).
func (c *FuturesClient) MarketOrder(symbol, side string, notionalUSDT float64) (map[string]any, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("binance futures client not configured (missing API key/secret)")
	}
	rules, err := c.LotRules(symbol)
	if err != nil {
		return nil, err
	}
	minN := rules.MinNotional
	if minN <= 0 {
		minN = 5
	}
	if notionalUSDT < minN {
		return nil, fmt.Errorf("notional %.2f below min %.2f USDT", notionalUSDT, minN)
	}
	price, err := c.MarkPrice(symbol)
	if err != nil {
		return nil, err
	}
	if price <= 0 {
		return nil, fmt.Errorf("invalid mark price")
	}
	qty := notionalUSDT / price
	return c.marketOrderOpenQty(symbol, side, qty)
}
