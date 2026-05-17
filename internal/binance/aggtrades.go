package binance

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// AggTrade is a normalized futures aggTrade row.
type AggTrade struct {
	ID              int64
	Price           float64
	Quantity        float64
	FirstTradeID    int64
	LastTradeID     int64
	Time            time.Time
	BuyerIsMaker    bool
}

// FetchAggTradesRange downloads aggTrades for [start, end] with 429 backoff.
func (c *FuturesClient) FetchAggTradesRange(symbol string, start, end time.Time) ([]AggTrade, error) {
	sym := strings.ToUpper(symbol)
	startMs := start.UnixMilli()
	endMs := end.UnixMilli()
	var out []AggTrade
	fromID := int64(0)

	for {
		q := url.Values{}
		q.Set("symbol", sym)
		q.Set("limit", "1000")
		if fromID > 0 {
			q.Set("fromId", strconv.FormatInt(fromID, 10))
		} else {
			q.Set("startTime", strconv.FormatInt(startMs, 10))
			q.Set("endTime", strconv.FormatInt(endMs, 10))
		}

		var rows []struct {
			AggID    int64  `json:"a"`
			Price    string `json:"p"`
			Qty      string `json:"q"`
			FirstID  int64  `json:"f"`
			LastID   int64  `json:"l"`
			Time     int64  `json:"T"`
			IsBuyerM bool   `json:"m"`
		}

		var respBody []byte
		var status int
		for attempt := 0; attempt < 8; attempt++ {
			resp, err := c.http.R().SetQueryString(q.Encode()).SetResult(&rows).Get(c.base + "/fapi/v1/aggTrades")
			if err != nil {
				return nil, err
			}
			status = resp.StatusCode()
			respBody = resp.Body()
			if status == 429 || status == 418 {
				wait := time.Duration(1+attempt) * time.Second
				time.Sleep(wait)
				continue
			}
			if status >= 300 {
				return nil, fmt.Errorf("aggTrades %s status=%d: %s", sym, status, string(respBody))
			}
			break
		}
		if status == 429 || status == 418 {
			return nil, fmt.Errorf("aggTrades %s: rate limited", sym)
		}
		if len(rows) == 0 {
			break
		}

		lastTime := rows[len(rows)-1].Time
		for _, r := range rows {
			if r.Time < startMs {
				continue
			}
			if r.Time > endMs {
				continue
			}
			price, _ := strconv.ParseFloat(r.Price, 64)
			qty, _ := strconv.ParseFloat(r.Qty, 64)
			out = append(out, AggTrade{
				ID:           r.AggID,
				Price:        price,
				Quantity:     qty,
				FirstTradeID: r.FirstID,
				LastTradeID:  r.LastID,
				Time:         time.UnixMilli(r.Time).UTC(),
				BuyerIsMaker: r.IsBuyerM,
			})
		}

		if lastTime >= endMs || len(rows) < 1000 {
			break
		}
		fromID = rows[len(rows)-1].AggID + 1
		time.Sleep(120 * time.Millisecond)
	}
	return out, nil
}
