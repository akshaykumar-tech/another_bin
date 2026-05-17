package whale

import (
	"fmt"
	"math"
	"time"
)

// Pre-trade gate: mega captures had dead/flat tape before entry (13 May / 14 May analysis).
func (d *BurstDetector) passesPreTradeFilters(now time.Time) bool {
	if reason := d.explainPreTradeReject(now); reason != "" {
		d.lastPumpReject = reason
		return false
	}
	return true
}

func (d *BurstDetector) explainPreTradeReject(now time.Time) string {
	if !d.cfg.PreTradeEnabled {
		return ""
	}
	win := d.cfg.PreTradeWindowMs
	if win <= 0 {
		win = 60_000
	}
	from := now.Add(-time.Duration(win) * time.Millisecond)
	// Exclude current 1s burst leg — only tape *before* the spike.
	preEnd := now.Add(-time.Second)

	quiet60 := notionalBetween(d.ticks, from, preEnd)
	if maxQ := d.cfg.MaxQuiet60sUSDT; maxQ > 0 && quiet60 > maxQ {
		return fmt.Sprintf("pre_trade quiet60 $%.0f > max $%.0f", quiet60, maxQ)
	}

	if maxR := d.cfg.MaxRange60sPct; maxR > 0 {
		rng := priceRangePctBetween(d.ticks, from, preEnd)
		if rng > maxR {
			return fmt.Sprintf("pre_trade range60 %.2f%% > max %.2f%%", rng, maxR)
		}
	}

	if max1s := d.cfg.MaxPrior1sMove60sPct; max1s > 0 {
		prior := max1sMoveBetween(d.ticks, from, preEnd)
		if prior > max1s {
			return fmt.Sprintf("pre_trade prior_1s %.2f%% > max %.2f%%", prior, max1s)
		}
	}

	if maxT := d.cfg.MaxTrades60s; maxT > 0 {
		n := tradeCountBetween(d.ticks, from, preEnd)
		if n > maxT {
			return fmt.Sprintf("pre_trade trades60 %d > max %d", n, maxT)
		}
	}

	return ""
}

func priceRangePctBetween(ticks []tradeTick, from, to time.Time) float64 {
	hi, lo := 0.0, math.MaxFloat64
	for _, t := range ticks {
		if t.at.Before(from) || t.at.After(to) {
			continue
		}
		if t.price > hi {
			hi = t.price
		}
		if t.price < lo {
			lo = t.price
		}
	}
	if lo <= 0 || hi <= 0 || lo == math.MaxFloat64 {
		return 0
	}
	mid := (hi + lo) / 2
	return (hi - lo) / mid * 100
}

func max1sMoveBetween(ticks []tradeTick, from, to time.Time) float64 {
	var best float64
	for i := 0; i < len(ticks); i++ {
		if ticks[i].at.Before(from) || ticks[i].at.After(to) {
			continue
		}
		p0 := ticks[i].price
		if p0 <= 0 {
			continue
		}
		end := ticks[i].at.Add(time.Second)
		for j := i; j < len(ticks) && !ticks[j].at.After(end); j++ {
			if ticks[j].at.After(to) {
				break
			}
			pct := math.Abs((ticks[j].price - p0) / p0 * 100)
			if pct > best {
				best = pct
			}
		}
	}
	return best
}

func tradeCountBetween(ticks []tradeTick, from, to time.Time) int {
	n := 0
	for _, t := range ticks {
		if !t.at.Before(from) && !t.at.After(to) {
			n++
		}
	}
	return n
}
