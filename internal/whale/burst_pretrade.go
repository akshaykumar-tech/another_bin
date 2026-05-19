package whale

import (
	"fmt"
	"math"
	"time"

	"crypto_announcements_go/internal/binance"
)

// PreTradeSnap is tape state in the window before the burst second (last 1s excluded).
type PreTradeSnap struct {
	Quiet60, Quiet30   float64
	Range60, Range30   float64
	Prior1s            float64
	Trades60, Trades30 int
	FlatMega           bool
	ElevatedMega       bool
	RejectReason       string
}

// preTradeSnap is an alias for internal use.
type preTradeSnap = PreTradeSnap

// PreTradeAt computes pre-trade metrics at entry time from historical aggTrades.
func PreTradeAt(cfg BurstConfig, trades []binance.AggTrade, at time.Time) PreTradeSnap {
	d := NewBurstDetector(cfg)
	for _, tr := range trades {
		if tr.Time.After(at) {
			break
		}
		d.ticks = append(d.ticks, tradeTick{
			at: tr.Time, price: tr.Price, qty: tr.Quantity, buyAggressive: !tr.BuyerIsMaker,
		})
	}
	snap := d.preTradeSnap(at)
	snap.FlatMega = matchesFlatMegaProfile(snap)
	snap.ElevatedMega = matchesElevatedMegaProfile(cfg, snap)
	snap.RejectReason = d.explainPreTradeReject(at)
	return snap
}

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
	snap := d.preTradeSnap(now)
	if d.cfg.PreTradeMegaOnly {
		if matchesFlatMegaProfile(snap) || matchesElevatedMegaProfile(d.cfg, snap) {
			return ""
		}
		return "pre_trade mega_only: tape not flat-mega or elevated-mega profile"
	}
	return d.explainPreTradeRejectLegacy(snap)
}

func (d *BurstDetector) preTradeSnap(now time.Time) PreTradeSnap {
	win := d.cfg.PreTradeWindowMs
	if win <= 0 {
		win = 60_000
	}
	shortMs := d.cfg.PreTradeShortWindowMs
	if shortMs <= 0 {
		shortMs = 30_000
	}
	from := now.Add(-time.Duration(win) * time.Millisecond)
	shortFrom := now.Add(-time.Duration(shortMs) * time.Millisecond)
	preEnd := now.Add(-time.Second)

	return PreTradeSnap{
		Quiet60:   notionalBetween(d.ticks, from, preEnd),
		Quiet30:   notionalBetween(d.ticks, shortFrom, preEnd),
		Range60:   priceRangePctBetween(d.ticks, from, preEnd),
		Range30:   priceRangePctBetween(d.ticks, shortFrom, preEnd),
		Prior1s:   max1sMoveBetween(d.ticks, from, preEnd),
		Trades60:  tradeCountBetween(d.ticks, from, preEnd),
		Trades30:  tradeCountBetween(d.ticks, shortFrom, preEnd),
	}
}

// matchesFlatMegaProfile: coordinated dump / thin tape (SYS & MLN @ 13 May 13:30).
func matchesFlatMegaProfile(s PreTradeSnap) bool {
	return s.Range60 <= 0.45 &&
		s.Range30 <= 0.28 &&
		s.Prior1s <= 0.12 &&
		s.Quiet30 <= 6000 &&
		s.Quiet60 <= 5000 &&
		s.Trades30 <= 25
}

// matchesElevatedMegaProfile: violent pump with busier tape (AIGEN 14 May 15:30).
// Min quiet floors drop 15 May MLN chop that mimics elevated caps but on thinner tape.
func matchesElevatedMegaProfile(cfg BurstConfig, s PreTradeSnap) bool {
	min60 := cfg.MinQuiet60ElevatedUSDT
	if min60 <= 0 {
		min60 = 35_000
	}
	min30 := cfg.MinQuiet30ElevatedUSDT
	if min30 <= 0 {
		min30 = 12_000
	}
	return s.Range60 <= 0.76 &&
		s.Range30 <= 0.40 &&
		s.Prior1s <= 0.27 &&
		s.Quiet60 >= min60 && s.Quiet60 <= 40_000 &&
		s.Quiet30 >= min30 && s.Quiet30 <= 22_000 &&
		s.Trades60 <= 250
}

func (d *BurstDetector) explainPreTradeRejectLegacy(s PreTradeSnap) string {
	if maxQ := d.cfg.MaxQuiet60sUSDT; maxQ > 0 && s.Quiet60 > maxQ {
		return fmt.Sprintf("pre_trade quiet60 $%.0f > max $%.0f", s.Quiet60, maxQ)
	}
	if maxQ30 := d.cfg.MaxQuiet30sUSDT; maxQ30 > 0 && s.Quiet30 > maxQ30 {
		return fmt.Sprintf("pre_trade quiet30 $%.0f > max $%.0f", s.Quiet30, maxQ30)
	}
	if maxR30 := d.cfg.MaxRange30sPct; maxR30 > 0 && s.Range30 > maxR30 {
		return fmt.Sprintf("pre_trade range30 %.2f%% > max %.2f%%", s.Range30, maxR30)
	}
	if maxR := d.cfg.MaxRange60sPct; maxR > 0 && s.Range60 > maxR {
		return fmt.Sprintf("pre_trade range60 %.2f%% > max %.2f%%", s.Range60, maxR)
	}
	if max1s := d.cfg.MaxPrior1sMove60sPct; max1s > 0 && s.Prior1s > max1s {
		return fmt.Sprintf("pre_trade prior_1s %.2f%% > max %.2f%%", s.Prior1s, max1s)
	}
	if maxT := d.cfg.MaxTrades60s; maxT > 0 && s.Trades60 > maxT {
		return fmt.Sprintf("pre_trade trades60 %d > max %d", s.Trades60, maxT)
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
