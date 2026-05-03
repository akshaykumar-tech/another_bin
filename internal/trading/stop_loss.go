package trading

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"crypto_announcements_go/internal/model"
)

// ComputeStopLossPlan mirrors Rails AutoTrading::Orchestrator#merge_stop_loss_into_execution! price math.
func ComputeStopLossPlan(entry, pct float64, positionSide string, rules model.FuturesLotRules) (stopPriceStr, closeSide string, meta map[string]any, ok bool) {
	if entry <= 0 || pct <= 0 {
		return "", "", nil, false
	}
	var rawStop float64
	if positionSide == "long" {
		rawStop = entry * (1 - pct/100)
	} else {
		rawStop = entry * (1 + pct/100)
	}
	tick, err := strconv.ParseFloat(strings.TrimSpace(rules.PriceTickSize), 64)
	if err != nil || tick <= 0 {
		tick = 0.01
	}
	up := positionSide != "long"
	snapped := snapPriceToTick(rawStop, tick, up)
	stopPriceStr = formatPriceString(snapped, rules.PriceTickSize, rules.PricePrecision)
	if positionSide == "long" {
		closeSide = "SELL"
	} else {
		closeSide = "BUY"
	}
	meta = map[string]any{
		"stop_loss_percent": fmt.Sprintf("%.4f", pct),
		"stop_loss_price":   stopPriceStr,
		"stop_loss_side":    closeSide,
		"working_type":      "MARK_PRICE",
	}
	return stopPriceStr, closeSide, meta, true
}

func snapPriceToTick(price, tick float64, up bool) float64 {
	if tick <= 0 {
		return price
	}
	n := price / tick
	if up {
		return math.Ceil(n-1e-12) * tick
	}
	return math.Floor(n+1e-12) * tick
}

func formatPriceString(price float64, tickSizeStr string, pricePrec int) string {
	dec := decimalsFromTickString(tickSizeStr)
	if pricePrec >= 0 && pricePrec < dec {
		dec = pricePrec
	}
	s := strconv.FormatFloat(price, 'f', dec, 64)
	return trimTrailingZeros(s)
}

func decimalsFromTickString(tickSizeStr string) int {
	tickSizeStr = strings.TrimSpace(tickSizeStr)
	i := strings.IndexByte(tickSizeStr, '.')
	if i < 0 {
		return 0
	}
	return len(strings.TrimRight(tickSizeStr[i+1:], "0"))
}

func trimTrailingZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}
