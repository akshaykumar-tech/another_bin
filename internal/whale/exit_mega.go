package whale

import "time"

// megaExitStep updates trailing stop / TP for burst mega positions.
func megaExitStep(r Risk, pos *simPosition, price float64, at time.Time) (closed bool, reason string, partialPnL float64) {
	if pos.EntryPrice <= 0 || price <= 0 {
		return false, "", 0
	}

	activate := r.MegaTrailActivatePct / 100
	trailDist := r.MegaTrailDistancePct / 100
	tpPct := r.MegaTakeProfitPct / 100
	slPct := r.MegaStopLossPercent / 100
	if slPct <= 0 {
		slPct = r.StopLossPercent / 100
	}
	partialFrac := r.PartialExitFraction
	partialMin := r.MegaPartialMinPct / 100
	if partialMin <= 0 {
		partialMin = r.TakeProfitPercent1 / 100
	}

	var ch float64
	var peakCh float64
	if pos.Side == SideBuy {
		ch = (price - pos.EntryPrice) / pos.EntryPrice
		if price > pos.PeakPrice {
			pos.PeakPrice = price
		}
		peakCh = (pos.PeakPrice - pos.EntryPrice) / pos.EntryPrice
	} else {
		ch = (pos.EntryPrice - price) / pos.EntryPrice
		if pos.PeakPrice <= 0 || price < pos.PeakPrice {
			pos.PeakPrice = price
		}
		peakCh = (pos.EntryPrice - pos.PeakPrice) / pos.EntryPrice
	}

	if ch <= -slPct {
		return true, "sl", 0
	}
	if ch >= tpPct {
		return true, "mega_tp", 0
	}

	// Chop killer: mega pumps show +2% quickly; flat timeouts had move30 <1%.
	if win := r.MegaConfirmWindowMs; win > 0 {
		minPct := r.MegaConfirmMinFavorablePct
		if minPct <= 0 {
			minPct = 2.0
		}
		if at.Sub(pos.OpenedAt) >= time.Duration(win)*time.Millisecond &&
			peakCh*100 < minPct && !pos.Partial {
			return true, "no_mega", 0
		}
	}

	// Partial only after trail arms and move is large enough (don't halve before the run).
	if !pos.Partial && ch >= activate && ch >= partialMin {
		pos.Partial = true
		return false, "", pos.NotionalUSDT() * partialFrac * partialMin
	}

	if ch >= activate {
		dist := trailDist
		if r.MegaTrailWidenPeakPct > 0 && peakCh*100 >= r.MegaTrailWidenPeakPct {
			w := r.MegaTrailWidenDistPct / 100
			if w > dist {
				dist = w
			}
		}
		holdOK := r.MegaTrailMinHoldMs <= 0 || at.Sub(pos.OpenedAt) >= time.Duration(r.MegaTrailMinHoldMs)*time.Millisecond
		if holdOK {
			if pos.Side == SideBuy {
				if price <= pos.PeakPrice*(1-dist) {
					return true, "trail", 0
				}
			} else if price >= pos.PeakPrice*(1+dist) {
				return true, "trail", 0
			}
		}
	}

	return false, "", 0
}

func standardExitStep(r Risk, pos *simPosition, price float64) (closed bool, reason string, partialPnL float64) {
	slPct := r.StopLossPercent / 100
	tp1Pct := r.TakeProfitPercent1 / 100
	tp2Pct := r.TakeProfitPercent2 / 100
	partialFrac := r.PartialExitFraction

	var ch float64
	if pos.Side == SideBuy {
		ch = (price - pos.EntryPrice) / pos.EntryPrice
		if ch <= -slPct {
			return true, "sl", 0
		}
		if !pos.Partial && ch >= tp1Pct {
			pos.Partial = true
			return false, "", pos.NotionalUSDT() * partialFrac * tp1Pct
		}
		if pos.Partial && ch >= tp2Pct {
			return true, "tp2", 0
		}
	} else {
		ch = (pos.EntryPrice - price) / pos.EntryPrice
		if ch <= -slPct {
			return true, "sl", 0
		}
		if !pos.Partial && ch >= tp1Pct {
			pos.Partial = true
			return false, "", pos.NotionalUSDT() * partialFrac * tp1Pct
		}
		if pos.Partial && ch >= tp2Pct {
			return true, "tp2", 0
		}
	}
	return false, "", 0
}

func closeSimPnL(r Risk, pos *simPosition, price float64) float64 {
	partialFrac := r.PartialExitFraction
	rem := 1.0
	if pos.Partial {
		rem = 1.0 - partialFrac
	}
	var ch float64
	if pos.Side == SideBuy {
		ch = (price - pos.EntryPrice) / pos.EntryPrice
	} else {
		ch = (pos.EntryPrice - price) / pos.EntryPrice
	}
	return pos.NotionalUSDT() * rem * ch
}
