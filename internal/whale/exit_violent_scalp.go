package whale

import "time"

// violentScalpExitStep: short violent dumps — tight trail + early stall exit (no 10m bleed).
func violentScalpExitStep(r Risk, pos *simPosition, price float64, at time.Time) (closed bool, reason string, partialPnL float64) {
	if pos.EntryPrice <= 0 || price <= 0 {
		return false, "", 0
	}

	stallMs := r.ViolentScalpStallMs
	if stallMs <= 0 {
		stallMs = 20_000
	}
	stallMin := r.ViolentScalpStallMinPct / 100
	if stallMin <= 0 {
		stallMin = 0.0018
	}
	activate := r.ViolentScalpTrailActivatePct / 100
	if activate <= 0 {
		activate = 0.0025
	}
	trailDist := r.ViolentScalpTrailDistPct / 100
	if trailDist <= 0 {
		trailDist = 0.0012
	}
	tpPct := r.ViolentScalpTPPct / 100
	if tpPct <= 0 {
		tpPct = 0.03
	}
	slPct := megaStopLossPct(r, pos)

	var ch, peakCh float64
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

	holdGrace := r.MegaTrailMinHoldMs
	if holdGrace <= 0 {
		holdGrace = 2000
	}
	elapsed := at.Sub(pos.OpenedAt)

	if ch <= -slPct && elapsed >= time.Duration(holdGrace)*time.Millisecond {
		return true, "sl", 0
	}
	if ch >= tpPct {
		return true, "scalp_tp", 0
	}

	// Dead dump: no follow-through in first N seconds → scratch (NOM-style timeouts).
	if elapsed >= time.Duration(stallMs)*time.Millisecond && peakCh < stallMin {
		return true, "stall", 0
	}

	// Trail only while in profit — avoids locking a loss on a small bounce (TAKE Jun2).
	if ch > 0 && ch >= activate {
		if pos.Side == SideBuy {
			if price <= pos.PeakPrice*(1-trailDist) {
				return true, "trail", 0
			}
		} else if price >= pos.PeakPrice*(1+trailDist) {
			return true, "trail", 0
		}
	}

	return false, "", 0
}

func useViolentScalp(r Risk, pos *simPosition) bool {
	if pos == nil || !r.ViolentScalpEnabled {
		return false
	}
	min := r.ViolentScalpMinMovePct
	if min <= 0 {
		min = 1.8
	}
	max := r.ViolentScalpMaxMovePct
	if max <= 0 {
		max = 8.0
	}
	return pos.SignalAbsMovePct >= min && pos.SignalAbsMovePct <= max
}

func positionExitStep(cfg Config, pos *simPosition, price float64, at time.Time) (closed bool, reason string, partial float64) {
	r := cfg.RiskForExit()
	if useViolentScalp(r, pos) {
		return violentScalpExitStep(r, pos, price, at)
	}
	if pos.MegaExit {
		return megaExitStep(r, pos, price, at)
	}
	return standardExitStep(r, pos, price)
}

func positionMaxHold(cfg Config, pos *simPosition) time.Duration {
	if pos == nil {
		return backtestHoldTimeout
	}
	r := cfg.Risk
	if useViolentScalp(r, pos) && r.ViolentScalpMaxHoldMs > 0 {
		return time.Duration(r.ViolentScalpMaxHoldMs) * time.Millisecond
	}
	return backtestHoldTimeout
}
