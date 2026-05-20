package whale

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// BurstDetector catches the start of violent 1s moves (100ms spike + volume).
type BurstDetector struct {
	cfg BurstConfig

	mu             sync.Mutex
	ticks          []tradeTick
	lastFire       time.Time
	lastPumpReject string // set when pump_only filters block (diagnostics)
}

func NewBurstDetector(cfg BurstConfig) *BurstDetector {
	return &BurstDetector{cfg: cfg}
}

func (d *BurstDetector) lookbackDur() time.Duration {
	ms := d.cfg.SecWindowMs
	if d.cfg.TrendWindowMs > ms {
		ms = d.cfg.TrendWindowMs
	}
	if d.cfg.QuietBeforeMs > ms {
		ms = d.cfg.QuietBeforeMs
	}
	if d.cfg.PreTradeEnabled && d.cfg.PreTradeWindowMs > ms {
		ms = d.cfg.PreTradeWindowMs
	}
	if d.cfg.PreTradeEnabled && d.cfg.PreTradeLongWindowMs > ms {
		ms = d.cfg.PreTradeLongWindowMs
	}
	if ms < 1000 {
		ms = 1000
	}
	return time.Duration(ms) * time.Millisecond
}

func (d *BurstDetector) OnAggTrade(price, qty float64, buyerIsMaker bool, at time.Time) *Signal {
	if price <= 0 || qty <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if cd := d.cfg.SignalCooldownMs; cd > 0 && !d.lastFire.IsZero() &&
		at.Sub(d.lastFire) < time.Duration(cd)*time.Millisecond {
		return nil
	}

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
	if len(d.ticks) < 2 {
		return nil
	}
	return d.evaluate(at)
}

func (d *BurstDetector) evaluate(now time.Time) *Signal {
	fastDur := time.Duration(d.cfg.FastWindowMs) * time.Millisecond
	secDur := time.Duration(d.cfg.SecWindowMs) * time.Millisecond
	fastStart := now.Add(-fastDur)
	secStart := now.Add(-secDur)

	var fastTicks, secTicks []tradeTick
	for _, t := range d.ticks {
		if !t.at.Before(fastStart) {
			fastTicks = append(fastTicks, t)
		}
		if !t.at.Before(secStart) {
			secTicks = append(secTicks, t)
		}
	}
	if len(fastTicks) < 2 || len(secTicks) < 2 {
		return nil
	}

	fp0 := fastTicks[0].price
	fp1 := fastTicks[len(fastTicks)-1].price
	if fp0 <= 0 {
		return nil
	}
	fastMove := (fp1 - fp0) / fp0 * 100

	sp0 := secTicks[0].price
	sp1 := secTicks[len(secTicks)-1].price
	if sp0 <= 0 {
		return nil
	}
	secMove := (sp1 - sp0) / sp0 * 100

	fastNotional := notional(fastTicks)
	secNotional := notional(secTicks)

	absFast := math.Abs(fastMove)
	absSec := math.Abs(secMove)

	if d.cfg.MaxSecMovePct > 0 && absSec > d.cfg.MaxSecMovePct {
		return nil
	}

	minFast := d.cfg.MinFastMovePct
	if minFast <= 0 {
		minFast = 0.5
	}
	minSecN := d.cfg.MinSecNotionalUSDT
	if minSecN <= 0 {
		minSecN = 8_000
	}
	minFastN := d.cfg.MinFastNotionalUSDT
	if minFastN <= 0 {
		minFastN = 3_000
	}
	minSecMove := d.cfg.MinSecMovePct
	if minSecMove < 0 {
		minSecMove = 0
	}

	burst := absFast >= minFast &&
		fastNotional >= minFastN &&
		secNotional >= minSecN &&
		absSec >= minSecMove

	if !burst {
		return d.tryCascade(now)
	}

	side := d.dominantSide(fastTicks, secTicks, secMove)
	if side == "" {
		return nil
	}

	if d.cfg.PumpOnly {
		d.lastPumpReject = ""
		if maxN := d.cfg.MaxSecNotionalUSDT; maxN > 0 && secNotional > maxN {
			d.lastPumpReject = fmt.Sprintf("sec_notional $%.0f > max $%.0f", secNotional, maxN)
			return nil
		}
		if !d.passesPumpFilters(now, side, fastMove, secMove, fastNotional, secNotional, secStart, fastStart) {
			return nil
		}
		if !d.passesPreTradeFilters(now) {
			return nil
		}
	}

	d.lastFire = now
	mega := true // burst entries always use mega trailing exit

	return &Signal{
		Kind:      SignalBurst,
		Side:      side,
		MovePct:   secMove,
		FastMove:  fastMove,
		SecVolume: secNotional,
		FlashMode: "pump",
		Mega:      mega,
		RecvAt:    now,
	}
}

func (d *BurstDetector) tryCascade(now time.Time) *Signal {
	ms := d.cfg.CascadeWindowMs
	if ms <= 0 {
		return nil
	}
	minMove := d.cfg.MinCascadeMovePct
	if minMove <= 0 {
		minMove = 2.5
	}
	minN := d.cfg.MinCascadeNotionalUSDT
	if minN <= 0 {
		minN = 12_000
	}
	start := now.Add(-time.Duration(ms) * time.Millisecond)
	var ticks []tradeTick
	for _, t := range d.ticks {
		if !t.at.Before(start) {
			ticks = append(ticks, t)
		}
	}
	if len(ticks) < 2 {
		return nil
	}
	p0 := ticks[0].price
	p1 := ticks[len(ticks)-1].price
	if p0 <= 0 {
		return nil
	}
	move := (p1 - p0) / p0 * 100
	secN := notional(ticks)
	absMove := math.Abs(move)
	if absMove < minMove || secN < minN {
		return nil
	}
	maxEntry := d.cfg.MaxCascadeMovePct
	if maxEntry <= 0 {
		maxEntry = 4.0
	}
	if absMove > maxEntry {
		d.lastPumpReject = fmt.Sprintf("cascade_move %.2f%% > max %.2f%%", absMove, maxEntry)
		return nil
	}
	side := d.dominantSide(ticks, ticks, move)
	if side == "" {
		return nil
	}
	if d.cfg.PumpOnly {
		d.lastPumpReject = ""
		if maxN := d.cfg.MaxSecNotionalUSDT; maxN > 0 && secN > maxN {
			d.lastPumpReject = fmt.Sprintf("sec_notional $%.0f > max $%.0f", secN, maxN)
			return nil
		}
		if d.cfg.TrendWindowMs > 0 && d.cfg.MaxCounterTrendPct > 0 {
			trendStart := now.Add(-time.Duration(d.cfg.TrendWindowMs) * time.Millisecond)
			trendMove := priceMoveBetween(d.ticks, trendStart, now)
			limit := d.cfg.MaxCounterTrendPct
			if side == SideBuy && trendMove < -limit {
				d.lastPumpReject = fmt.Sprintf("counter_trend buy vs 60s %.2f%%", trendMove)
				return nil
			}
			if side == SideSell && trendMove > limit {
				d.lastPumpReject = fmt.Sprintf("counter_trend sell vs 60s %.2f%%", trendMove)
				return nil
			}
		}
		if !d.passesPreTradeFilters(now) {
			return nil
		}
	}
	d.lastFire = now
	return &Signal{
		Kind: SignalBurst, Side: side, MovePct: move, FastMove: move, SecVolume: secN,
		FlashMode: "cascade", Mega: true, RecvAt: now,
	}
}

func (d *BurstDetector) passesPumpFilters(now time.Time, side Side, fastMove, secMove, fastN, secN float64, secStart, fastStart time.Time) bool {
	if reason := d.explainPumpReject(now, side, fastMove, secMove, fastN, secN, secStart, fastStart); reason != "" {
		d.lastPumpReject = reason
		return false
	}
	return true
}

func notionalBetween(ticks []tradeTick, from, to time.Time) float64 {
	var sum float64
	for _, t := range ticks {
		if !t.at.Before(from) && t.at.Before(to) {
			sum += t.price * t.qty
		}
	}
	return sum
}

func priceMoveBetween(ticks []tradeTick, from, to time.Time) float64 {
	var first, last float64
	for _, t := range ticks {
		if t.at.Before(from) || t.at.After(to) {
			continue
		}
		if first <= 0 {
			first = t.price
		}
		last = t.price
	}
	if first <= 0 || last <= 0 {
		return 0
	}
	return (last - first) / first * 100
}

func (d *BurstDetector) dominantSide(fast, sec []tradeTick, secMove float64) Side {
	ticks := fast
	if len(fast) < 2 {
		ticks = sec
	}
	var buy, sell float64
	for _, t := range ticks {
		v := t.price * t.qty
		if t.buyAggressive {
			buy += v
		} else {
			sell += v
		}
	}
	total := buy + sell
	dom := d.cfg.SideDominancePct / 100
	if dom <= 0 {
		dom = 0.52
	}
	if total <= 0 {
		if secMove > 0 {
			return SideBuy
		}
		if secMove < 0 {
			return SideSell
		}
		return ""
	}
	if buy >= total*dom && secMove > 0 {
		return SideBuy
	}
	if sell >= total*dom && secMove < 0 {
		return SideSell
	}
	return ""
}
