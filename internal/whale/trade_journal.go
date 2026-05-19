package whale

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// TradeJournal appends one line per entry/exit (cat-friendly, like backtest logs).
type TradeJournal struct {
	path string
	mu   sync.Mutex

	cumulativePnL float64
	tradeCount    int
}

func NewTradeJournal(path string) (*TradeJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "whale-trades.log"
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	j := &TradeJournal{path: path}
	if err := j.ensureHeader(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *TradeJournal) ensureHeader() error {
	st, err := os.Stat(j.path)
	if err == nil && st.Size() > 0 {
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return j.appendLine("# whale trade journal — ENTRY / PARTIAL / EXIT (tab-separated; cumulative_usdt = running total)")
}

func (j *TradeJournal) LogEntry(sig *Signal, entryPrice, margin float64, leverage int, simMode string) {
	if j == nil || sig == nil {
		return
	}
	kind := string(sig.Kind)
	if sig.Kind == SignalBurst {
		kind = "burst"
	}
	lev := leverage
	if lev <= 0 {
		lev = 1
	}
	notional := margin * float64(lev)
	line := fmt.Sprintf("ENTRY\t%s\t%s\t%s\t%s\tfast=%.2f%%\t1s=%.2f%%\tvol=$%.0f\tentry=%.6f\tmargin=%.2f\tlev=%dx\tnotional=%.2f\tsim=%s",
		time.Now().UTC().Format(time.RFC3339),
		sig.Side, sig.Symbol, kind,
		sig.FastMove, sig.MovePct, sig.SecVolume, entryPrice, margin, lev, notional, simMode)
	_ = j.appendLine(line)
}

func (j *TradeJournal) LogPartial(sym string, side Side, partialUSDT float64, at time.Time) {
	if j == nil || partialUSDT == 0 {
		return
	}
	j.mu.Lock()
	j.cumulativePnL += partialUSDT
	cum := j.cumulativePnL
	j.mu.Unlock()
	line := fmt.Sprintf("PARTIAL\t%s\t%s\t%s\tpnl_usdt=%+.2f\tcumulative_usdt=%+.2f",
		at.UTC().Format(time.RFC3339), side, sym, partialUSDT, cum)
	_ = j.appendLine(line)
}

func (j *TradeJournal) LogExit(r Risk, pos *simPosition, exitPrice float64, at time.Time, reason string, partialAlready float64) {
	if j == nil || pos == nil {
		return
	}
	pnl := closeSimPnL(r, pos, exitPrice)
	if partialAlready > 0 {
		pnl += partialAlready
	}
	ch := priceChangePct(pos.Side, pos.EntryPrice, exitPrice)
	hold := at.Sub(pos.OpenedAt).Round(time.Second)

	j.mu.Lock()
	j.cumulativePnL += pnl
	j.tradeCount++
	cum := j.cumulativePnL
	n := j.tradeCount
	j.mu.Unlock()

	lev := pos.Leverage
	if lev <= 0 {
		lev = 1
	}
	line := fmt.Sprintf("EXIT\t%s\t%s\t%s\treason=%s\texit=%.6f\tpnl_pct=%+.2f\tpnl_usdt=%+.2f\tlev=%dx\thold=%s\tcumulative_usdt=%+.2f\ttrades=%d",
		at.UTC().Format(time.RFC3339),
		pos.Side, pos.Symbol, reason,
		exitPrice, ch, pnl, lev, hold, cum, n)
	_ = j.appendLine(line)
}

func priceChangePct(side Side, entry, exit float64) float64 {
	if entry <= 0 || exit <= 0 {
		return 0
	}
	if side == SideBuy {
		return (exit - entry) / entry * 100
	}
	return (entry - exit) / entry * 100
}

func (j *TradeJournal) appendLine(line string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, line)
	return err
}

func (j *TradeJournal) Path() string {
	if j == nil {
		return ""
	}
	return j.path
}
