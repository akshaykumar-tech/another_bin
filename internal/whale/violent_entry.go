package whale

import (
	"fmt"
	"time"
)

// explainViolentSellEntryReject gates coordinated dump shorts (TAKE-style, not NOM chop).
func (d *BurstDetector) explainViolentSellEntryReject(now time.Time, side Side, fastMove, secMove, secN float64) string {
	if side != SideSell {
		return ""
	}
	absSec := secMove
	if absSec < 0 {
		absSec = -absSec
	}
	if !d.isViolentCoordinatedBurst(absSec, secN) {
		return ""
	}

	if minR := d.cfg.MinViolentFastSecRatio; minR > 0 {
		r := fastSecMomentumRatio(fastMove, secMove)
		if r < minR {
			return fmt.Sprintf("violent_fast_sec_ratio %.2f < min %.2f (fast=%.2f%% 1s=%.2f%%)",
				r, minR, fastMove, secMove)
		}
	}

	snap := d.preTradeSnap(now)
	if maxR := d.cfg.MaxPreDumpRange60sPct; maxR > 0 && snap.Range60 > maxR {
		return fmt.Sprintf("pre_dump_range60 %.2f%% > max %.2f%%", snap.Range60, maxR)
	}
	if minR := d.cfg.MinViolentSecQuiet60Ratio; minR > 0 && snap.Quiet60 > 0 && snap.Quiet60 < flatQuiet60Ceil(d.cfg) {
		if secN/snap.Quiet60 < minR {
			return fmt.Sprintf("violent_sec_q60 %.1f < min %.1f (sec=$%.0f q60=$%.0f)",
				secN/snap.Quiet60, minR, secN, snap.Quiet60)
		}
	}
	return ""
}
