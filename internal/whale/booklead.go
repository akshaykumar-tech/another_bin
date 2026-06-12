package whale

import (
	"math"
	"strconv"
	"sync"
	"time"
)

// BookLeadDetector predicts violent moves from order-book imbalance + trade flow
// before price has already made a large move.
type BookLeadDetector struct {
	cfg BookLeadConfig

	mu       sync.Mutex
	ticks    []tradeTick
	lastBids [][]string
	lastAsks [][]string
	lastFire time.Time
}

func NewBookLeadDetector(cfg BookLeadConfig) *BookLeadDetector {
	return &BookLeadDetector{cfg: cfg}
}

func (d *BookLeadDetector) OnDepth(bids, asks [][]string, at time.Time) *Signal {
	if len(bids) == 0 || len(asks) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastBids = cloneLevels(bids)
	d.lastAsks = cloneLevels(asks)
	d.pruneTicks(at)
	return d.evaluate(at)
}

func (d *BookLeadDetector) OnAggTrade(price, qty float64, buyerIsMaker bool, at time.Time) *Signal {
	if price <= 0 || qty <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ticks = append(d.ticks, tradeTick{
		at: at, price: price, qty: qty, buyAggressive: !buyerIsMaker,
	})
	d.pruneTicks(at)
	if len(d.lastBids) == 0 || len(d.lastAsks) == 0 {
		return nil
	}
	return d.evaluate(at)
}

func (d *BookLeadDetector) pruneTicks(at time.Time) {
	cut := at.Add(-time.Duration(d.cfg.WindowMs) * time.Millisecond)
	i := 0
	for i < len(d.ticks) && d.ticks[i].at.Before(cut) {
		i++
	}
	if i > 0 {
		d.ticks = d.ticks[i:]
	}
}

func (d *BookLeadDetector) evaluate(now time.Time) *Signal {
	if !d.lastFire.IsZero() && now.Sub(d.lastFire) < time.Duration(d.cfg.SignalCooldownMs)*time.Millisecond {
		return nil
	}
	mid, ok := midFromBook(d.lastBids, d.lastAsks)
	if !ok || mid <= 0 {
		return nil
	}

	levels := d.cfg.DepthLevels
	if levels <= 0 {
		levels = 10
	}

	bidNotional := sideNotional(d.lastBids, levels)
	askNotional := sideNotional(d.lastAsks, levels)
	if d.cfg.MinBookSideUSDT > 0 {
		if bidNotional < d.cfg.MinBookSideUSDT || askNotional < d.cfg.MinBookSideUSDT {
			return nil
		}
	}

	buyFlow, sellFlow := tradeFlow(d.ticks)
	tradeTotal := buyFlow + sellFlow
	if tradeTotal < d.cfg.MinTradeNotionalUSDT {
		return nil
	}

	movePct := priceMovePct(d.ticks)
	// MaxEntryMovePct <= 0 disables skip — capture violent moves as they build (not only pre-move).
	if d.cfg.MaxEntryMovePct > 0 && math.Abs(movePct) > d.cfg.MaxEntryMovePct {
		return nil
	}

	sweep := d.cfg.SweepBandPct
	if sweep <= 0 {
		sweep = 3.0
	}
	askThin := askNotionalWithinPct(d.lastAsks, mid, sweep)
	bidThin := bidNotionalWithinPct(d.lastBids, mid, sweep)

	dom := d.cfg.TradeDominancePct / 100
	if dom <= 0 {
		dom = 0.6
	}
	ratioMin := d.cfg.MinImbalanceRatio

	// Long: optional thin asks + optional bid-heavy book + aggressive buying
	longBook := bookFavorsLong(bidNotional, askNotional, ratioMin)
	longThin := thinSideOK(askThin, d.cfg.MaxThinSideUSDT)
	longFlow := buyFlow >= tradeTotal*dom && buyFlow >= d.cfg.MinTradeNotionalUSDT &&
		buyFlow > sellFlow
	if longBook && longThin && longFlow {
		d.lastFire = now
		return &Signal{
			Kind:           SignalBookLead,
			Side:           SideBuy,
			ImbalanceRatio: bidNotional / askNotional,
			BidNotional:    bidNotional,
			AskNotional:    askNotional,
			ThinSideUSDT:   askThin,
			TradeFlowUSDT:  buyFlow,
			SecVolume:      tradeTotal,
			MovePct:        movePct,
			BookMode:       bookMode(bidNotional, askNotional, buyFlow, sellFlow),
			Mega:           bidNotional/askNotional >= ratioMin*1.5 && buyFlow >= d.cfg.MinTradeNotionalUSDT*2,
			RecvAt:         now,
		}
	}

	// Short: optional thin bids + optional ask-heavy book + aggressive selling
	shortBook := bookFavorsShort(bidNotional, askNotional, ratioMin)
	shortThin := thinSideOK(bidThin, d.cfg.MaxThinSideUSDT)
	shortFlow := sellFlow >= tradeTotal*dom && sellFlow >= d.cfg.MinTradeNotionalUSDT &&
		sellFlow > buyFlow
	if shortBook && shortThin && shortFlow {
		d.lastFire = now
		return &Signal{
			Kind:           SignalBookLead,
			Side:           SideSell,
			ImbalanceRatio: askNotional / bidNotional,
			BidNotional:    bidNotional,
			AskNotional:    askNotional,
			ThinSideUSDT:   bidThin,
			TradeFlowUSDT:  sellFlow,
			SecVolume:      tradeTotal,
			MovePct:        movePct,
			BookMode:       bookMode(askNotional, bidNotional, sellFlow, buyFlow),
			Mega:           askNotional/bidNotional >= ratioMin*1.5 && sellFlow >= d.cfg.MinTradeNotionalUSDT*2,
			RecvAt:         now,
		}
	}
	return nil
}

func bookFavorsLong(bid, ask, ratioMin float64) bool {
	if ratioMin <= 0 {
		return bid >= ask
	}
	return bid >= ask*ratioMin
}

func bookFavorsShort(bid, ask, ratioMin float64) bool {
	if ratioMin <= 0 {
		return ask >= bid
	}
	return ask >= bid*ratioMin
}

func thinSideOK(thinUSDT, maxThin float64) bool {
	if maxThin <= 0 {
		return true
	}
	return thinUSDT > 0 && thinUSDT <= maxThin
}

func bookMode(strongBook, weakBook, strongFlow, weakFlow float64) string {
	if strongBook/weakBook >= 4 && strongFlow >= weakFlow*2 {
		return "violent"
	}
	return "lead"
}

func midFromBook(bids, asks [][]string) (float64, bool) {
	bp, ok1 := levelPrice(bids, 0)
	ap, ok2 := levelPrice(asks, 0)
	if !ok1 || !ok2 {
		return 0, false
	}
	return (bp + ap) / 2, true
}

func levelPrice(levels [][]string, i int) (float64, bool) {
	if i >= len(levels) || len(levels[i]) < 1 {
		return 0, false
	}
	p, err := strconv.ParseFloat(levels[i][0], 64)
	return p, err == nil && p > 0
}

func sideNotional(levels [][]string, maxLevels int) float64 {
	n := len(levels)
	if n > maxLevels {
		n = maxLevels
	}
	var sum float64
	for i := 0; i < n; i++ {
		if len(levels[i]) < 2 {
			continue
		}
		p, err1 := strconv.ParseFloat(levels[i][0], 64)
		q, err2 := strconv.ParseFloat(levels[i][1], 64)
		if err1 != nil || err2 != nil || p <= 0 || q <= 0 {
			continue
		}
		sum += p * q
	}
	return sum
}

func askNotionalWithinPct(asks [][]string, mid, pct float64) float64 {
	cap := mid * (1 + pct/100)
	var sum float64
	for _, lv := range asks {
		if len(lv) < 2 {
			continue
		}
		p, err1 := strconv.ParseFloat(lv[0], 64)
		q, err2 := strconv.ParseFloat(lv[1], 64)
		if err1 != nil || err2 != nil || p <= 0 || q <= 0 {
			continue
		}
		if p > cap {
			break
		}
		sum += p * q
	}
	return sum
}

func bidNotionalWithinPct(bids [][]string, mid, pct float64) float64 {
	floor := mid * (1 - pct/100)
	var sum float64
	for _, lv := range bids {
		if len(lv) < 2 {
			continue
		}
		p, err1 := strconv.ParseFloat(lv[0], 64)
		q, err2 := strconv.ParseFloat(lv[1], 64)
		if err1 != nil || err2 != nil || p <= 0 || q <= 0 {
			continue
		}
		if p < floor {
			break
		}
		sum += p * q
	}
	return sum
}

func tradeFlow(ticks []tradeTick) (buy, sell float64) {
	for _, t := range ticks {
		v := t.price * t.qty
		if t.buyAggressive {
			buy += v
		} else {
			sell += v
		}
	}
	return buy, sell
}

func priceMovePct(ticks []tradeTick) float64 {
	if len(ticks) < 2 {
		return 0
	}
	p0 := ticks[0].price
	p1 := ticks[len(ticks)-1].price
	if p0 <= 0 {
		return 0
	}
	return (p1 - p0) / p0 * 100
}

func cloneLevels(in [][]string) [][]string {
	out := make([][]string, len(in))
	for i, lv := range in {
		cp := make([]string, len(lv))
		copy(cp, lv)
		out[i] = cp
	}
	return out
}
