package whale

import (
	"fmt"
	"math"
	"time"

	"crypto_announcements_go/internal/binance"
)

// PumpDiag explains why burst did or did not fire near a target time.
type PumpDiag struct {
	At           time.Time
	Fired        bool
	Side         Side
	FastMove     float64
	SecMove      float64
	SecNotional  float64
	RejectReason string
}

func aggToTicks(trades []binance.AggTrade) []tradeTick {
	out := make([]tradeTick, len(trades))
	for i, tr := range trades {
		out[i] = tradeTick{
			at: tr.Time, price: tr.Price, qty: tr.Quantity, buyAggressive: !tr.BuyerIsMaker,
		}
	}
	return out
}

// appendTick warms the detector tape without evaluating signals (fast replay).
func (d *BurstDetector) appendTick(price, qty float64, buyerIsMaker bool, at time.Time) {
	if price <= 0 || qty <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ticks = append(d.ticks, tradeTick{
		at: at, price: price, qty: qty, buyAggressive: !buyerIsMaker,
	})
	cut := at.Add(-d.lookbackDur())
	i := 0
	for i < len(d.ticks) && d.ticks[i].at.Before(cut) {
		i++
	}
	if i > 0 {
		d.ticks = d.ticks[i:]
	}
}

// DiagnoseBurstNear replays trades and returns the closest burst evaluation to target (±search).
func DiagnoseBurstNear(cfg BurstConfig, trades []binance.AggTrade, target time.Time, search time.Duration) (PumpDiag, float64, float64) {
	return diagnoseBurstNear(cfg, trades, target, search, false)
}

// DiagnoseBurstNearFast warms tape before the probe window, then evaluates only ±search (much faster).
func DiagnoseBurstNearFast(cfg BurstConfig, trades []binance.AggTrade, target time.Time, search time.Duration) (PumpDiag, float64, float64) {
	return diagnoseBurstNear(cfg, trades, target, search, true)
}

func diagnoseBurstNear(cfg BurstConfig, trades []binance.AggTrade, target time.Time, search time.Duration, fast bool) (PumpDiag, float64, float64) {
	ticks := aggToTicks(trades)
	d := NewBurstDetector(cfg)
	var best PumpDiag
	bestDist := search + time.Second
	probeStart := target.Add(-search)
	probeEnd := target.Add(search)

	if fast {
		var warm []tradeTick
		for _, t := range ticks {
			if !t.at.Before(probeStart) {
				break
			}
			warm = append(warm, t)
		}
		if len(warm) > 0 {
			d.mu.Lock()
			d.ticks = warm
			cut := probeStart.Add(-d.lookbackDur())
			i := 0
			for i < len(d.ticks) && d.ticks[i].at.Before(cut) {
				i++
			}
			if i > 0 {
				d.ticks = d.ticks[i:]
			}
			d.mu.Unlock()
		}
	}

	for _, t := range ticks {
		if t.at.Before(probeStart) {
			continue
		}
		if t.at.After(probeEnd) {
			break
		}
		sig := d.OnAggTrade(t.price, t.qty, !t.buyAggressive, t.at)
		dist := absDuration(t.at.Sub(target))
		if dist > search {
			continue
		}
		diag := PumpDiag{At: t.at}
		if sig != nil {
			diag.Fired = true
			diag.Side = sig.Side
			diag.FastMove = sig.FastMove
			diag.SecMove = sig.MovePct
			diag.SecNotional = sig.SecVolume
		} else if cfg.PumpOnly && d.lastPumpReject != "" {
			diag.RejectReason = d.lastPumpReject
		}
		if sig != nil && !best.Fired {
			bestDist = dist
			best = diag
		} else if !best.Fired && (dist < bestDist || (diag.RejectReason != "" && best.RejectReason == "")) {
			bestDist = dist
			best = diag
		}
	}

	max5s, max30s := maxMoveAfter(ticks, target, 5*time.Second, 30*time.Second)
	return best, max5s, max30s
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func maxMoveAfter(ticks []tradeTick, from time.Time, w5, w30 time.Duration) (float64, float64) {
	p0 := priceAt(ticks, from)
	if p0 <= 0 {
		return 0, 0
	}
	var max5, max30 float64
	for _, t := range ticks {
		if t.at.Before(from) {
			continue
		}
		dt := t.at.Sub(from)
		if dt > w30 {
			break
		}
		ch := math.Abs((t.price - p0) / p0 * 100)
		if dt <= w5 && ch > max5 {
			max5 = ch
		}
		if ch > max30 {
			max30 = ch
		}
	}
	return max5, max30
}

func priceAt(ticks []tradeTick, at time.Time) float64 {
	var p float64
	for _, t := range ticks {
		if t.at.After(at) {
			break
		}
		p = t.price
	}
	return p
}

// ExplainPumpReject sets lastPumpReject when pump filters fail (for diagnostics).
// fastSecMomentumRatio is |fast%|/|1s%| when both legs share sign; 0 if misaligned.
func fastSecMomentumRatio(fastMove, secMove float64) float64 {
	absFast := math.Abs(fastMove)
	absSec := math.Abs(secMove)
	if absFast <= 0 || absSec <= 0 {
		return 0
	}
	if (fastMove > 0) != (secMove > 0) {
		return 0
	}
	return absFast / absSec
}

func (d *BurstDetector) explainPumpReject(now time.Time, side Side, fastMove, secMove, fastN, secN float64, secStart, fastStart time.Time) string {
	absFast := math.Abs(fastMove)
	absSec := math.Abs(secMove)
	violent := d.isViolentCoordinatedBurst(absSec, secN)

	if minR := d.cfg.MinFastSecRatio; minR > 0 && !violent {
		r := fastSecMomentumRatio(fastMove, secMove)
		if r < minR {
			return fmt.Sprintf("fast_sec_ratio %.2f < min %.2f (fast=%.2f%% 1s=%.2f%%)", r, minR, fastMove, secMove)
		}
	}

	if d.cfg.EarlyCaptureAll {
		minN := d.cfg.MinSecNotionalUSDT
		if minN <= 0 {
			minN = 10_000
		}
		if secN < minN {
			return fmt.Sprintf("sec_notional $%.0f < min $%.0f", secN, minN)
		}
		maxSec := d.maxEntrySecMovePct(absSec, secN)
		if secMove > 0 && secMove > maxSec {
			return fmt.Sprintf("sec_move +%.2f%% > max %.2f%%", secMove, maxSec)
		}
		if secMove < 0 && secMove < -maxSec {
			return fmt.Sprintf("sec_move %.2f%% < max -%.2f%%", secMove, maxSec)
		}
		return ""
	}

	if violent {
		if reason := d.explainViolentSellEntryReject(now, side, fastMove, secMove, secN); reason != "" {
			return reason
		}
		if d.cfg.TrendWindowMs > 0 && d.cfg.MaxCounterTrendPct > 0 {
			trendStart := now.Add(-time.Duration(d.cfg.TrendWindowMs) * time.Millisecond)
			trendMove := priceMoveBetween(d.ticks, trendStart, now)
			limit := d.cfg.MaxCounterTrendPct
			if side == SideBuy && trendMove < -limit {
				return fmt.Sprintf("counter_trend buy vs 60s %.2f%%", trendMove)
			}
			if side == SideSell && trendMove > limit {
				return fmt.Sprintf("counter_trend sell vs 60s %.2f%%", trendMove)
			}
		}
		maxSec := d.maxEntrySecMovePct(absSec, secN)
		if secMove > 0 && secMove > maxSec {
			return fmt.Sprintf("sec_move +%.2f%% > max %.2f%%", secMove, maxSec)
		}
		if secMove < 0 && secMove < -maxSec {
			return fmt.Sprintf("sec_move %.2f%% < max -%.2f%%", secMove, maxSec)
		}
		return ""
	}

	if !violent {
		if maxFast := d.cfg.MaxFastMovePct; maxFast > 0 && absFast > maxFast {
			return fmt.Sprintf("fast_move %.2f%% > max %.2f%%", absFast, maxFast)
		}
	}
	if align := d.cfg.MinMomentumAlign; align > 0 && absFast > 0 && absSec/absFast < align {
		return fmt.Sprintf("momentum_align %.2f < min %.2f", absSec/absFast, align)
	}
	if share := d.cfg.MinFastVolSharePct; share > 0 && secN > 0 && fastN/secN*100 < share {
		return fmt.Sprintf("fast_vol_share %.0f%% < min %.0f%%", fastN/secN*100, share)
	}
	var quietN float64
	if d.cfg.QuietBeforeMs > 0 {
		qStart := now.Add(-time.Duration(d.cfg.QuietBeforeMs) * time.Millisecond)
		quietN = notionalBetween(d.ticks, qStart, secStart)
	}
	snap := d.preTradeSnap(now)
	maxQ5 := d.maxQuietBeforeUSDT(snap)
	if maxQ5 > 0 && quietN > maxQ5 {
		return fmt.Sprintf("quiet_before $%.0f > max $%.0f", quietN, maxQ5)
	}
	imp := d.cfg.MinBurstImpulse
	if matchesStandardFlatMegaProfile(d.cfg, snap) {
		if fi := d.cfg.MinBurstImpulseFlat; fi > 0 {
			imp = fi
		}
	}
	if imp > 0 {
		if quietN < 500 {
			// dead tape before burst — allow (ATA-style cascade from silence)
		} else if secN/quietN < imp {
			return fmt.Sprintf("burst_impulse %.1f < min %.1f (sec=$%.0f quiet=$%.0f)", secN/quietN, imp, secN, quietN)
		}
	}
	if accel := d.cfg.MinVolumeAccel; accel > 0 {
		priorN := notionalBetween(d.ticks, secStart, fastStart)
		if priorN <= 0 || fastN/priorN < accel {
			return fmt.Sprintf("volume_accel %.1f < min %.1f", fastN/priorN, accel)
		}
	}
	if d.cfg.TrendWindowMs > 0 && d.cfg.MaxCounterTrendPct > 0 {
		trendStart := now.Add(-time.Duration(d.cfg.TrendWindowMs) * time.Millisecond)
		trendMove := priceMoveBetween(d.ticks, trendStart, now)
		limit := d.cfg.MaxCounterTrendPct
		if side == SideBuy && trendMove < -limit {
			return fmt.Sprintf("counter_trend buy vs 60s %.2f%%", trendMove)
		}
		if side == SideSell && trendMove > limit {
			return fmt.Sprintf("counter_trend sell vs 60s %.2f%%", trendMove)
		}
	}
	maxSec := d.maxEntrySecMovePct(absSec, secN)
	if secMove > 0 && secMove > maxSec {
		return fmt.Sprintf("sec_move +%.2f%% > max %.2f%%", secMove, maxSec)
	}
	if secMove < 0 && secMove < -maxSec {
		return fmt.Sprintf("sec_move %.2f%% < max -%.2f%%", secMove, maxSec)
	}
	if !d.isViolentCoordinatedBurst(absSec, secN) {
		if reason := d.explainLiquidityBlastReject(now, secN); reason != "" {
			return reason
		}
	}
	return ""
}

func (d *BurstDetector) maxQuietBeforeUSDT(snap PreTradeSnap) float64 {
	if matchesUltraFlatMegaProfile(d.cfg, snap) {
		if v := d.cfg.MaxQuietBeforeUltraUSDT; v > 0 {
			return v
		}
	}
	if matchesStandardFlatMegaProfile(d.cfg, snap) {
		if v := d.cfg.MaxQuietBeforeFlatUSDT; v > 0 {
			return v
		}
	}
	return d.cfg.MaxQuietBeforeUSDT
}

// explainLiquidityBlastReject blocks normal-liquidity fake spikes (no entry delay).
// Ultra (MLN): high sec/q60 ratio. Standard flat (SYS): lower ratio — global 11 wrongly blocked SYS.
func (d *BurstDetector) explainLiquidityBlastReject(now time.Time, secN float64) string {
	snap := d.preTradeSnap(now)
	if secN <= 0 || snap.Quiet60 <= 0 || snap.Quiet60 >= flatQuiet60Ceil(d.cfg) {
		return ""
	}
	var minR float64
	switch {
	case matchesUltraFlatMegaProfile(d.cfg, snap):
		minR = d.cfg.MinSecQuiet60RatioUltra
		if minR <= 0 {
			minR = d.cfg.MinSecQuiet60Ratio
		}
	case matchesStandardFlatMegaProfile(d.cfg, snap):
		minR = d.cfg.MinSecQuiet60RatioFlat
		if minR <= 0 {
			minR = 5.5
		}
	default:
		minR = d.cfg.MinSecQuiet60Ratio
	}
	if minR > 0 && secN/snap.Quiet60 < minR {
		return fmt.Sprintf("sec_vs_quiet60 %.1f < min %.1f (sec=$%.0f q60=$%.0f)",
			secN/snap.Quiet60, minR, secN, snap.Quiet60)
	}
	return ""
}
