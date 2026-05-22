package whale

import (
	"fmt"
	"time"
)

// Violent coordinated dump: 13 May/22 May style — entire 1s leg is 2–16% (not 0.5% early entry).
// MLN/SYS enter at ~0.5% in 1s; COOKIE/DODO/EPIC/HEI skip max_entry 0.7% and flat-mega pretrade.

func (d *BurstDetector) isViolentCoordinatedBurst(absSecMove, secNotional float64) bool {
	if !d.cfg.ViolentBurstEnabled {
		return false
	}
	minS := d.cfg.MinViolentSecMovePct
	if minS <= 0 {
		minS = 2.0
	}
	maxS := d.cfg.MaxViolentSecMovePct
	if maxS <= 0 {
		maxS = 16.0
	}
	minN := d.cfg.MinViolentSecNotionalUSDT
	if minN <= 0 {
		minN = 12_000
	}
	return absSecMove >= minS && absSecMove <= maxS && secNotional >= minN
}

func (d *BurstDetector) passesPreTradeForBurst(now time.Time, absSecMove, secNotional float64) bool {
	if !d.cfg.PreTradeEnabled {
		return true
	}
	if d.isViolentCoordinatedBurst(absSecMove, secNotional) {
		if reason := ExplainViolentPreTradeReject(d.cfg, d.preTradeSnap(now)); reason != "" {
			d.lastPumpReject = reason
			return false
		}
		return true
	}
	return d.passesPreTradeFilters(now)
}

// ExplainViolentPreTradeReject reports why a violent dump fails long-window gates.
func ExplainViolentPreTradeReject(cfg BurstConfig, s PreTradeSnap) string {
	if cfg.PreTradeLongWindowMs <= 0 {
		return ""
	}
	maxR := cfg.MaxRangeLongViolentPct
	if maxR <= 0 {
		maxR = 9.0
	}
	if s.Range2h > maxR {
		return fmt.Sprintf("violent long range %.2f%% > max %.2f%%", s.Range2h, maxR)
	}
	// 60s pre-dump tape (not 3h): 11:00–13:30 morning spikes must not block 13:30 dump.
	max1s := cfg.MaxPrior1sViolentPct
	if max1s <= 0 {
		max1s = cfg.MaxPrior1sLongViolentPct
	}
	if max1s <= 0 {
		max1s = 0.45
	}
	if s.Prior1s > max1s {
		return fmt.Sprintf("violent prior_1s %.2f%% > max %.2f%%", s.Prior1s, max1s)
	}
	if minN := cfg.MinNotionalLongViolentUSDT; minN > 0 && s.Quiet2h < minN {
		return fmt.Sprintf("violent long notional $%.0f < min $%.0f", s.Quiet2h, minN)
	}
	maxQ60 := cfg.MaxQuiet60ViolentUSDT
	if maxQ60 <= 0 {
		maxQ60 = 5000
	}
	if s.Quiet60 > maxQ60 {
		return fmt.Sprintf("violent q60 $%.0f > max $%.0f (busy tape)", s.Quiet60, maxQ60)
	}
	return ""
}

// maxEntrySecMovePct returns the 1s move cap at entry (0.7% early mega vs 16% violent dump).
func (d *BurstDetector) maxEntrySecMovePct(absSecMove, secNotional float64) float64 {
	if d.isViolentCoordinatedBurst(absSecMove, secNotional) {
		if v := d.cfg.MaxViolentSecMovePct; v > 0 {
			return v
		}
		return 16.0
	}
	if v := d.cfg.MaxEntrySecMovePct; v > 0 {
		return v
	}
	return 1.5
}
