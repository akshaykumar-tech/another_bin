package whale

import "time"

func (e *Executor) earlyTakeProfitPct() float64 {
	if e == nil {
		return 0
	}
	return e.cfg.Early.TakeProfitPct
}

func (e *Executor) earlyExitDeadline(pos *simPosition) time.Time {
	if pos == nil {
		return time.Time{}
	}
	return pos.exitDeadline(e.dryHoldDuration(SignalEarly))
}

func (e *Executor) earlyLiveExitDeadline(ep *earlyLivePosition) time.Time {
	if ep == nil {
		return time.Time{}
	}
	if !ep.ScheduledExitAt.IsZero() {
		return ep.ScheduledExitAt
	}
	hold := e.dryHoldDuration(SignalEarly)
	return ep.OpenedAt.Add(hold)
}

func earlyFavorableMovePct(side Side, entry, price float64) float64 {
	if entry <= 0 || price <= 0 {
		return 0
	}
	return priceChangePct(side, entry, price)
}

func (e *Executor) earlyTakeProfitHit(side Side, entry, price float64) bool {
	tp := e.earlyTakeProfitPct()
	if tp <= 0 || entry <= 0 || price <= 0 {
		return false
	}
	return earlyFavorableMovePct(side, entry, price) >= tp
}
