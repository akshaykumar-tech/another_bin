package binance

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type volumeSymbol struct {
	symbol string
	vol    float64
}

// USDTPerpetualSymbols returns TRADING USDT-M perpetual symbols from cache.
func (c *FuturesClient) USDTPerpetualSymbols() []string {
	out := make([]string, 0, len(c.ruleMap))
	for sym, ok := range c.ruleMap {
		if !ok {
			continue
		}
		if strings.HasSuffix(sym, "USDT") && !strings.Contains(sym, "_") {
			out = append(out, sym)
		}
	}
	sort.Strings(out)
	return out
}

// LowestVolumeUSDTPerpetuals returns the n lowest 24h quote-volume USDT perpetuals.
func (c *FuturesClient) LowestVolumeUSDTPerpetuals(n int) ([]string, error) {
	ranked, err := c.rankUSDTPerpetualsByVolume()
	if err != nil {
		return nil, err
	}
	if n > len(ranked) {
		n = len(ranked)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = ranked[i].symbol
	}
	return out, nil
}

// MidVolumeUSDTPerpetuals skips the skip lowest-volume symbols and returns the next n.
func (c *FuturesClient) MidVolumeUSDTPerpetuals(skip, n int) ([]string, error) {
	ranked, err := c.rankUSDTPerpetualsByVolume()
	if err != nil {
		return nil, err
	}
	if skip > len(ranked) {
		skip = len(ranked)
	}
	slice := ranked[skip:]
	if n > len(slice) {
		n = len(slice)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = slice[i].symbol
	}
	return out, nil
}

func (c *FuturesClient) rankUSDTPerpetualsByVolume() ([]volumeSymbol, error) {
	var rows []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}
	resp, err := c.http.R().SetResult(&rows).Get(c.base + "/fapi/v1/ticker/24hr")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() >= 300 {
		return nil, fmt.Errorf("ticker/24hr status=%d", resp.StatusCode())
	}
	perps := make(map[string]bool, len(c.ruleMap))
	for sym, ok := range c.ruleMap {
		if ok && strings.HasSuffix(sym, "USDT") && !strings.Contains(sym, "_") {
			perps[sym] = true
		}
	}
	var ranked []volumeSymbol
	for _, r := range rows {
		sym := strings.ToUpper(r.Symbol)
		if !perps[sym] {
			continue
		}
		vol, _ := strconv.ParseFloat(strings.TrimSpace(r.QuoteVolume), 64)
		ranked = append(ranked, volumeSymbol{symbol: sym, vol: vol})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].vol == ranked[j].vol {
			return ranked[i].symbol < ranked[j].symbol
		}
		return ranked[i].vol < ranked[j].vol
	})
	return ranked, nil
}
