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

// DiagnoseBurstNear replays trades and returns the closest burst evaluation to target (±search).
func DiagnoseBurstNear(cfg BurstConfig, trades []binance.AggTrade, target time.Time, search time.Duration) (PumpDiag, float64, float64) {
	ticks := aggToTicks(trades)
	d := NewBurstDetector(cfg)
	var best PumpDiag
	bestDist := search + time.Second

	for _, t := range ticks {
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
		if dist < bestDist || (dist == bestDist && sig != nil && !best.Fired) {
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
func (d *BurstDetector) explainPumpReject(now time.Time, side Side, fastMove, secMove, fastN, secN float64, secStart, fastStart time.Time) string {
	absFast := math.Abs(fastMove)
	absSec := math.Abs(secMove)

	if maxFast := d.cfg.MaxFastMovePct; maxFast > 0 && absFast > maxFast {
		return fmt.Sprintf("fast_move %.2f%% > max %.2f%%", absFast, maxFast)
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
	if d.cfg.MaxQuietBeforeUSDT > 0 && quietN > d.cfg.MaxQuietBeforeUSDT {
		return fmt.Sprintf("quiet_before $%.0f > max $%.0f", quietN, d.cfg.MaxQuietBeforeUSDT)
	}
	if imp := d.cfg.MinBurstImpulse; imp > 0 {
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
	maxSec := d.cfg.MaxEntrySecMovePct
	if maxSec <= 0 {
		maxSec = 1.5
	}
	if secMove > 0 && secMove > maxSec {
		return fmt.Sprintf("sec_move +%.2f%% > max %.2f%%", secMove, maxSec)
	}
	if secMove < 0 && secMove < -maxSec {
		return fmt.Sprintf("sec_move %.2f%% < max -%.2f%%", secMove, maxSec)
	}
	return ""
}
