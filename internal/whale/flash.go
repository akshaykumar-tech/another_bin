package whale

import (
	"math"
	"sync"
	"time"
)

// FlashDetector catches violent price moves (e.g. 5–10% within ~1s) from aggTrade flow.
type FlashDetector struct {
	cfg FlashConfig

	mu       sync.Mutex
	ticks    []tradeTick
	lastFire time.Time
}

type tradeTick struct {
	at            time.Time
	price         float64
	qty           float64
	buyAggressive bool
}

func NewFlashDetector(cfg FlashConfig) *FlashDetector {
	return &FlashDetector{cfg: cfg}
}

func (d *FlashDetector) OnAggTrade(price, qty float64, buyerIsMaker bool, at time.Time) *Signal {
	if price <= 0 || qty <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	d.ticks = append(d.ticks, tradeTick{
		at: at, price: price, qty: qty, buyAggressive: !buyerIsMaker,
	})
	secCut := at.Add(-time.Duration(d.cfg.SecWindowMs) * time.Millisecond)
	i := 0
	for i < len(d.ticks) && d.ticks[i].at.Before(secCut) {
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

func (d *FlashDetector) evaluate(now time.Time) *Signal {
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
	if len(secTicks) < 2 {
		return nil
	}

	p0 := secTicks[0].price
	p1 := secTicks[len(secTicks)-1].price
	if p0 <= 0 {
		return nil
	}
	secMove := (p1 - p0) / p0 * 100

	var fastMove float64
	if len(fastTicks) >= 2 {
		fp0 := fastTicks[0].price
		fp1 := fastTicks[len(fastTicks)-1].price
		if fp0 > 0 {
			fastMove = (fp1 - fp0) / fp0 * 100
		}
	}

	secNotional := notional(secTicks)
	fastNotional := notional(fastTicks)

	absSec := math.Abs(secMove)
	if absSec > d.cfg.MaxSecMovePct {
		return nil // already moved too much — late
	}

	// Main: full flash (e.g. 5%+ in 1 second) with real volume
	fullFlash := absSec >= d.cfg.MinSecMovePct && secNotional >= d.cfg.MinSecNotionalUSDT

	// Early: violent 100ms leg while 1s move is already building (catch start of 5–10%)
	earlyFlash := len(fastTicks) >= 2 &&
		math.Abs(fastMove) >= d.cfg.MinFastMovePct &&
		absSec >= d.cfg.EarlySecMovePct &&
		fastNotional >= d.cfg.MinFastNotionalUSDT

	if !fullFlash && !earlyFlash {
		return nil
	}

	side := d.dominantSide(fastTicks, secTicks, secMove)
	if side == "" {
		return nil
	}

	mode := "full"
	if earlyFlash && !fullFlash {
		mode = "early"
	}

	return &Signal{
		Kind:      SignalFlash,
		Side:      side,
		MovePct:   secMove,
		FastMove:  fastMove,
		SecVolume: secNotional,
		FlashMode: mode,
		Mega:      absSec >= d.cfg.MinSecMovePct*1.2,
		RecvAt:    now,
	}
}

func notional(ticks []tradeTick) float64 {
	var n float64
	for _, t := range ticks {
		n += t.price * t.qty
	}
	return n
}

func (d *FlashDetector) dominantSide(fast, sec []tradeTick, secMove float64) Side {
	ticks := sec
	if len(fast) >= 2 {
		ticks = fast
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
		dom = 0.55
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
