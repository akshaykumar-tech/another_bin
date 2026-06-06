package whale

import (
	"fmt"
	"sort"
	"time"

	"crypto_announcements_go/internal/binance"
)

// EarlyWatchConfig: 1h tape scan → enter ~lead before expected burst, hold fixed duration.
type EarlyWatchConfig struct {
	Rule            string  `yaml:"rule"` // quiet_flat | dead_tape
	LeadMinutes     int     `yaml:"lead_minutes"`
	HoldMinutes     int     `yaml:"hold_minutes"`
	ScanStepMinutes int     `yaml:"scan_step_minutes"`
	MaxQuiet60USDT  float64 `yaml:"max_quiet60_usdt"`
	MaxRange60Pct   float64 `yaml:"max_range60_pct"`
	DeadMaxQuiet60  float64 `yaml:"dead_max_quiet60_usdt"`
	DeadMaxTrades30 int     `yaml:"dead_max_trades30"`
	// Direction: oracle (backtest cheat), trend_1h, short_only, long_only, first_move
	Direction    string  `yaml:"direction"`
	FirstMovePct float64 `yaml:"first_move_pct"`
	MinAmpPct    float64 `yaml:"min_amp_pct"` // events mode: label extreme 1s legs
	MinVolAccel  float64 `yaml:"min_vol_accel"` // reverse dry path: recent 20m vol / prior 20m vol
	// Live dry paths (cmd/whale-early, EARLY_DRY_* env):
	DrySameEnabled         bool    `yaml:"dry_same"`
	DryReverseLimitEnabled bool    `yaml:"dry_reverse_limit"`
	LiveReverseLimitEnabled bool   `yaml:"live_reverse_limit"` // Binance GTX limit → hold → market exit
	LiveLimitFillSec       float64 `yaml:"limit_fill_sec"` // max wait for reverse limit fill (default 1800)
	TakeProfitPct          float64 `yaml:"take_profit_pct"` // early same-dir: exit when favorable move >= this (0=off)
	// Backtest-only (whale-early-sim flags):
	ReverseTrade  bool    `yaml:"-"`
	FeeBpsPerSide float64 `yaml:"-"` // Binance taker per side; round-trip deducted from each trade
	LimitEntry    bool    `yaml:"-"` // post-only limit at signal price; skip if no fill within window
	LimitExit     bool    `yaml:"-"` // post-only limit at scheduled exit price; market fallback after window
	LimitFillSec  float64 `yaml:"-"` // max wait for limit fill (default 300s)
}

func (c EarlyWatchConfig) lead() time.Duration {
	if c.LeadMinutes <= 0 {
		return 20 * time.Minute
	}
	return time.Duration(c.LeadMinutes) * time.Minute
}

func (c EarlyWatchConfig) hold() time.Duration {
	if c.HoldMinutes <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(c.HoldMinutes) * time.Minute
}

func (c EarlyWatchConfig) scanStep() time.Duration {
	if c.ScanStepMinutes <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(c.ScanStepMinutes) * time.Minute
}

func (c *EarlyWatchConfig) ApplyDefaults() {
	if c.MaxQuiet60USDT <= 0 {
		c.MaxQuiet60USDT = 2000
	}
	if c.MaxRange60Pct <= 0 {
		c.MaxRange60Pct = 0.5
	}
	if c.DeadMaxQuiet60 <= 0 {
		c.DeadMaxQuiet60 = 1000
	}
	if c.DeadMaxTrades30 <= 0 {
		c.DeadMaxTrades30 = 5
	}
	if c.FirstMovePct <= 0 {
		c.FirstMovePct = 0.3
	}
	if c.MinAmpPct <= 0 {
		c.MinAmpPct = 4.0
	}
	if c.Direction == "" {
		c.Direction = "trend_1h"
	}
	if c.LiveLimitFillSec <= 0 {
		c.LiveLimitFillSec = 1800
	}
}

// EarlyTape is pre-entry tape state at scan time.
type EarlyTape struct {
	At       time.Time
	Snap     PreTradeSnap
	VolAccel float64
}

func BuildEarlyTape(burst BurstConfig, trades []binance.AggTrade, at time.Time) EarlyTape {
	return EarlyTape{
		At:       at,
		Snap:     PreTradeAt(burst, trades, at),
		VolAccel: earlyVolAccel(trades, at),
	}
}

func (c EarlyWatchConfig) RuleFires(t EarlyTape) bool {
	switch c.Rule {
	case "dead_tape":
		return t.Snap.Quiet60 < c.DeadMaxQuiet60 && t.Snap.Trades30 <= c.DeadMaxTrades30
	case "quiet_flat", "":
		return t.Snap.Quiet60 <= c.MaxQuiet60USDT && t.Snap.Range60 <= c.MaxRange60Pct
	default:
		return false
	}
}

// PassesFilters applies rule + optional extra gates (vol surge, etc.).
func (c EarlyWatchConfig) PassesFilters(t EarlyTape) bool {
	if !c.RuleFires(t) {
		return false
	}
	if c.MinVolAccel > 0 && t.VolAccel < c.MinVolAccel {
		return false
	}
	return true
}

func earlyVolAccel(trades []binance.AggTrade, at time.Time) float64 {
	a := earlyNotional(trades, at.Add(-20*time.Minute), at)
	b := earlyNotional(trades, at.Add(-40*time.Minute), at.Add(-20*time.Minute))
	if b <= 0 {
		return 0
	}
	return a / b
}

func earlyNotional(trades []binance.AggTrade, from, to time.Time) float64 {
	var s float64
	for _, tr := range trades {
		if !tr.Time.Before(from) && !tr.Time.After(to) {
			s += tr.Price * tr.Quantity
		}
	}
	return s
}

// EarlyDirection returns true for short.
func EarlyDirection(mode string, trades []binance.AggTrade, at time.Time, oracleNetPct float64) (short bool, ok bool) {
	switch mode {
	case "oracle":
		if oracleNetPct == 0 {
			return false, false
		}
		return oracleNetPct < 0, true
	case "short_only":
		return true, true
	case "long_only":
		return false, true
	case "trend_1h":
		p0 := earlyPriceAt(trades, at.Add(-time.Hour))
		p1 := earlyPriceAt(trades, at)
		if p0 <= 0 || p1 <= 0 {
			return false, false
		}
		return p1 < p0, true // downtrend → short
	default:
		return false, false
	}
}

// EarlyDirectionFirstMove waits up to wait for first ±movePct leg, returns short if down move.
func EarlyDirectionFirstMove(trades []binance.AggTrade, from time.Time, wait time.Duration, movePct float64) (short bool, entryAt time.Time, ok bool) {
	deadline := from.Add(wait)
	p0 := earlyPriceAt(trades, from)
	if p0 <= 0 {
		return false, time.Time{}, false
	}
	i0 := sort.Search(len(trades), func(i int) bool { return !trades[i].Time.Before(from) })
	for i := i0; i < len(trades); i++ {
		tr := trades[i]
		if tr.Time.After(deadline) {
			break
		}
		ch := (tr.Price - p0) / p0 * 100
		if ch >= movePct {
			return false, tr.Time, true
		}
		if ch <= -movePct {
			return true, tr.Time, true
		}
	}
	return false, time.Time{}, false
}

func earlyPriceAt(trades []binance.AggTrade, at time.Time) float64 {
	i := sort.Search(len(trades), func(j int) bool { return !trades[j].Time.Before(at) })
	if i >= len(trades) {
		return 0
	}
	return trades[i].Price
}

// earlyTryLimitFill simulates post-only limit at limitPx after signal time.
// Buy limit fills on trade <= limitPx; sell limit on trade >= limitPx.
func earlyTryLimitFill(trades []binance.AggTrade, from time.Time, window time.Duration, limitPx float64, short bool) (fillAt time.Time, ok bool) {
	if limitPx <= 0 || window <= 0 {
		return time.Time{}, false
	}
	deadline := from.Add(window)
	i := sort.Search(len(trades), func(j int) bool { return !trades[j].Time.Before(from) })
	for ; i < len(trades); i++ {
		tr := trades[i]
		if tr.Time.After(deadline) {
			break
		}
		if short {
			if tr.Price >= limitPx {
				return tr.Time, true
			}
		} else if tr.Price <= limitPx {
			return tr.Time, true
		}
	}
	return time.Time{}, false
}

// EarlyEntryDelay is the backtest/live offset from signal time to entry reference (fillcheck entryAt).
func EarlyEntryDelay(cfg Config) time.Duration {
	d := time.Duration(cfg.BacktestEntryDelayMs) * time.Millisecond
	if d <= 0 {
		d = 30 * time.Millisecond
	}
	return d
}

func earlyEntrySlippageBps(cfg Config) float64 {
	s := cfg.BacktestEntrySlippageBps
	if s <= 0 {
		s = 30
	}
	return s
}

// EarlySameDirEntryLimitPx is the fixed reverse limit price (fillcheck entry_px on same-dir tick entry).
func EarlySameDirEntryLimitPx(cfg Config, trades []binance.AggTrade, signalAt time.Time, sameDirShort bool) float64 {
	entryRefAt := signalAt.Add(EarlyEntryDelay(cfg))
	px := earlyPriceAt(trades, entryRefAt)
	if px <= 0 {
		return 0
	}
	return applyEarlySlip(px, sameDirShort, earlyEntrySlippageBps(cfg), true)
}

// EarlySameDirLimitPxFromSignal applies same-dir entry slip to the live signal price.
func EarlySameDirLimitPxFromSignal(cfg Config, sig *Signal) float64 {
	if sig == nil || sig.EntryPrice <= 0 {
		return 0
	}
	sameDirShort := sig.Side == SideSell
	return applyEarlySlip(sig.EntryPrice, sameDirShort, earlyEntrySlippageBps(cfg), true)
}

type EarlyTrade struct {
	Symbol    string
	Date      string
	SignalAt  time.Time
	BurstAt   time.Time // events mode: ≥minAmp 1s leg time (zero in scan mode)
	EntryAt   time.Time
	ExitAt    time.Time
	EntryPx   float64
	ExitPx    float64
	PctCh     float64
	Short     bool
	PnLUSDT   float64
	Rule      string
	Direction string
	Oracle    bool
}

type EarlyBacktestSummary struct {
	Signals     int
	Entries     int
	LimitMissed     int // signal fired but limit entry did not fill in window
	LimitExitMissed int // limit exit not filled; closed at market after window
	Wins        int
	PnLUSDT     float64
	Trades      []EarlyTrade
}

// RunEarlyBacktestScan walks tape on a fixed grid (live-like).
func RunEarlyBacktestScan(cfg Config, symbol string, trades []binance.AggTrade, date string) EarlyBacktestSummary {
	ew := cfg.Early
	ew.ApplyDefaults()
	if len(trades) < 100 {
		return EarlyBacktestSummary{}
	}

	margin := cfg.MarginUSDT
	if margin <= 0 {
		margin = 1
	}
	lev := cfg.Leverage
	if lev <= 0 {
		lev = 10
	}
	entryDelay := time.Duration(cfg.BacktestEntryDelayMs) * time.Millisecond
	if entryDelay <= 0 {
		entryDelay = 30 * time.Millisecond
	}
	exitDelay := time.Duration(cfg.BacktestExitDelayMs) * time.Millisecond
	if exitDelay <= 0 {
		exitDelay = 10 * time.Millisecond
	}
	slipIn := cfg.BacktestEntrySlippageBps
	if slipIn <= 0 {
		slipIn = 30
	}
	slipOut := cfg.BacktestExitSlippageBps
	if slipOut <= 0 {
		slipOut = 30
	}

	start := trades[0].Time.Add(2 * time.Hour)
	end := trades[len(trades)-1].Time.Add(-ew.hold())
	var out EarlyBacktestSummary
	var busyUntil time.Time
	cooldown := time.Duration(cfg.CooldownSec * float64(time.Second))
	if cooldown <= 0 {
		cooldown = 30 * time.Minute
	}

	for at := start; !at.After(end); at = at.Add(ew.scanStep()) {
		if !busyUntil.IsZero() && at.Before(busyUntil) {
			continue
		}
		tape := BuildEarlyTape(cfg.Burst, trades, at)
		if !ew.PassesFilters(tape) {
			continue
		}
		out.Signals++

		entAt := at
		short := false
		dirMode := ew.Direction

		if dirMode == "first_move" {
			s, moveAt, ok := EarlyDirectionFirstMove(trades, at, ew.lead(), ew.FirstMovePct)
			if !ok {
				continue
			}
			short = s
			entAt = moveAt
			dirMode = "first_move"
		} else {
			s, ok := EarlyDirection(ew.Direction, trades, at, 0)
			if !ok {
				continue
			}
			short = s
		}
		sameDirShort := short
		if ew.ReverseTrade {
			short = !short
		}

		signalAt := at
		entryRefAt := at.Add(entryDelay)

		var limitPx float64
		if ew.LimitEntry && ew.ReverseTrade {
			limitPx = EarlySameDirEntryLimitPx(cfg, trades, signalAt, sameDirShort)
		} else {
			limitPx = earlyPriceAt(trades, at)
		}
		if limitPx <= 0 {
			continue
		}

		var entPx float64
		if ew.LimitEntry {
			fillWindow := time.Duration(ew.LimitFillSec * float64(time.Second))
			if fillWindow <= 0 {
				fillWindow = 5 * time.Minute
			}
			if ew.ReverseTrade {
				// Block next signal like same-dir tick entry (entryRef + hold + cooldown).
				busyUntil = entryRefAt.Add(ew.hold()).Add(cooldown)
			}
			fillAt, filled := earlyTryLimitFill(trades, entryRefAt, fillWindow, limitPx, short)
			if !filled {
				out.LimitMissed++
				continue
			}
			entAt = fillAt
			entPx = limitPx // maker fill at limit, no entry slip
		} else {
			entAt = entAt.Add(entryDelay)
			entPx = earlyPriceAt(trades, entAt)
			if entPx <= 0 {
				continue
			}
			entPx = applyEarlySlip(entPx, short, slipIn, true)
		}
		exitAt := signalAt.Add(ew.hold())
		if !ew.LimitEntry {
			exitAt = entAt.Add(ew.hold())
		}
		fillWindow := time.Duration(ew.LimitFillSec * float64(time.Second))
		if fillWindow <= 0 {
			fillWindow = 5 * time.Minute
		}

		var exitPx float64
		if ew.LimitExit {
			exitLimitPx := earlyPriceAt(trades, exitAt)
			if exitLimitPx <= 0 {
				continue
			}
			// Close long = sell limit; close short = buy limit.
			exitCloseSell := !short
			fillAt, filled := earlyTryLimitFill(trades, exitAt, fillWindow, exitLimitPx, exitCloseSell)
			if filled {
				exitAt = fillAt
				exitPx = exitLimitPx
			} else {
				out.LimitExitMissed++
				fallback := exitAt.Add(fillWindow)
				exitPx = earlyPriceAt(trades, fallback.Add(exitDelay))
				if exitPx <= 0 {
					exitPx = trades[len(trades)-1].Price
				}
				exitPx = applyEarlySlip(exitPx, short, slipOut, false)
				exitAt = fallback
			}
		} else if ew.ReverseTrade && ew.LimitEntry {
			// fillcheck variant A: market exit @ same-dir scheduled exit tick price
			exitPx = earlyPriceAt(trades, exitAt.Add(exitDelay))
			if exitPx <= 0 {
				exitPx = trades[len(trades)-1].Price
			}
			exitPx = applyEarlySlip(exitPx, sameDirShort, slipOut, false)
		} else {
			exitPx = earlyPriceAt(trades, exitAt.Add(exitDelay))
			if exitPx <= 0 {
				exitPx = trades[len(trades)-1].Price
			}
			exitPx = applyEarlySlip(exitPx, short, slipOut, false)
		}

		ch := earlyPnLCh(entPx, exitPx, short)
		pnl := margin * float64(lev) * ch / 100
		if ew.FeeBpsPerSide > 0 {
			pnl -= margin * float64(lev) * ew.FeeBpsPerSide * 2 / 10000
		}
		out.Entries++
		out.PnLUSDT += pnl
		if pnl > 0 {
			out.Wins++
		}
		if !(ew.ReverseTrade && ew.LimitEntry) {
			busyUntil = exitAt.Add(cooldown)
		}
		out.Trades = append(out.Trades, EarlyTrade{
			Symbol: symbol, Date: date, SignalAt: at, EntryAt: entAt, ExitAt: exitAt,
			EntryPx: entPx, ExitPx: exitPx, PctCh: ch, Short: short, PnLUSDT: pnl,
			Rule: ew.Rule, Direction: dirMode,
		})
	}
	return out
}

// RunEarlyBacktestEvents replays only known ≥minAmp 1s legs (oracle-direction upper bound).
func RunEarlyBacktestEvents(cfg Config, symbol string, trades []binance.AggTrade, date string) EarlyBacktestSummary {
	ew := cfg.Early
	ew.ApplyDefaults()
	events := findEarlyExtreme1s(symbol, date, trades, ew.MinAmpPct)

	margin := cfg.MarginUSDT
	if margin <= 0 {
		margin = 1
	}
	lev := cfg.Leverage
	if lev <= 0 {
		lev = 10
	}
	entryDelay := time.Duration(cfg.BacktestEntryDelayMs) * time.Millisecond
	if entryDelay <= 0 {
		entryDelay = 30 * time.Millisecond
	}
	exitDelay := time.Duration(cfg.BacktestExitDelayMs) * time.Millisecond
	if exitDelay <= 0 {
		exitDelay = 10 * time.Millisecond
	}
	slipIn := cfg.BacktestEntrySlippageBps
	if slipIn <= 0 {
		slipIn = 30
	}
	slipOut := cfg.BacktestExitSlippageBps
	if slipOut <= 0 {
		slipOut = 30
	}

	var out EarlyBacktestSummary
	for _, ev := range events {
		at := ev.at.Add(-ew.lead())
		tape := BuildEarlyTape(cfg.Burst, trades, at)
		if !ew.PassesFilters(tape) {
			continue
		}
		out.Signals++

		short, ok := EarlyDirection(ew.Direction, trades, at, ev.netPct)
		if !ok {
			continue
		}
		entAt := at.Add(entryDelay)
		entPx := earlyPriceAt(trades, entAt)
		if entPx <= 0 {
			continue
		}
		entPx = applyEarlySlip(entPx, short, slipIn, true)
		exitAt := entAt.Add(ew.hold())
		exitPx := earlyPriceAt(trades, exitAt.Add(exitDelay))
		if exitPx <= 0 {
			continue
		}
		exitPx = applyEarlySlip(exitPx, short, slipOut, false)

		ch := earlyPnLCh(entPx, exitPx, short)
		pnl := margin * float64(lev) * ch / 100
		out.Entries++
		out.PnLUSDT += pnl
		if pnl > 0 {
			out.Wins++
		}
		out.Trades = append(out.Trades, EarlyTrade{
			Symbol: symbol, Date: date, SignalAt: at, BurstAt: ev.at,
			EntryAt: entAt, ExitAt: exitAt, EntryPx: entPx, ExitPx: exitPx,
			PctCh: ch, Short: short, PnLUSDT: pnl,
			Rule: ew.Rule, Direction: ew.Direction, Oracle: ew.Direction == "oracle",
		})
	}
	return out
}

func applyEarlySlip(px float64, short bool, bps float64, entry bool) float64 {
	if entry {
		if short {
			return px * (1 - bps/10000)
		}
		return px * (1 + bps/10000)
	}
	if short {
		return px * (1 + bps/10000)
	}
	return px * (1 - bps/10000)
}

func earlyPnLCh(entry, exit float64, short bool) float64 {
	if short {
		return (entry - exit) / entry * 100
	}
	return (exit - entry) / entry * 100
}

type earlyExtreme struct {
	at             time.Time
	ampPct, netPct float64
}

func findEarlyExtreme1s(sym, date string, trades []binance.AggTrade, minAmp float64) []earlyExtreme {
	_ = sym
	_ = date
	type bar struct {
		open, high, low, close float64
		n                      int
	}
	bars := map[int64]*bar{}
	for _, tr := range trades {
		sec := tr.Time.Unix()
		b, ok := bars[sec]
		if !ok {
			b = &bar{open: tr.Price, high: tr.Price, low: tr.Price, close: tr.Price}
			bars[sec] = b
		}
		if tr.Price > b.high {
			b.high = tr.Price
		}
		if tr.Price < b.low {
			b.low = tr.Price
		}
		b.close = tr.Price
		b.n++
	}
	keys := make([]int64, 0, len(bars))
	for k := range bars {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	var out []earlyExtreme
	var last time.Time
	for _, sec := range keys {
		b := bars[sec]
		if b.open <= 0 || b.n < 3 {
			continue
		}
		up := (b.high - b.open) / b.open * 100
		dn := (b.open - b.low) / b.open * 100
		amp := up
		if dn > amp {
			amp = dn
		}
		if amp < minAmp {
			continue
		}
		at := time.Unix(sec, 0).UTC()
		if !last.IsZero() && at.Sub(last) < 2*time.Minute {
			continue
		}
		last = at
		net := (b.close - b.open) / b.open * 100
		out = append(out, earlyExtreme{at, amp, net})
	}
	return out
}

func MergeEarlySummary(dst *EarlyBacktestSummary, src EarlyBacktestSummary) {
	dst.Signals += src.Signals
	dst.Entries += src.Entries
	dst.LimitMissed += src.LimitMissed
	dst.LimitExitMissed += src.LimitExitMissed
	dst.Wins += src.Wins
	dst.PnLUSDT += src.PnLUSDT
	dst.Trades = append(dst.Trades, src.Trades...)
}

func PrintEarlyTradeList(title string, trades []EarlyTrade) {
	fmt.Println(title)
	fmt.Printf("%-4s %-10s %-8s %-23s %-23s %-19s %-19s %-12s %-10s %-10s %7s %7s\n",
		"#", "symbol", "date", "signal_UTC", "entry_UTC", "exit_UTC", "burst_UTC", "side", "entry_px", "exit_px", "pct%", "pnl$")
	for i, t := range trades {
		burst := ""
		if !t.BurstAt.IsZero() {
			burst = t.BurstAt.Format("2006-01-02 15:04:05")
		}
		side := "LONG"
		if t.Short {
			side = "SHORT"
		}
		fmt.Printf("%-4d %-10s %-8s %-23s %-23s %-19s %-19s %-12s %-10.6f %-10.6f %7.2f %7.2f\n",
			i+1, t.Symbol, t.Date,
			t.SignalAt.Format("2006-01-02 15:04:05.000"),
			t.EntryAt.Format("2006-01-02 15:04:05.000"),
			t.ExitAt.Format("2006-01-02 15:04:05"),
			burst, side, t.EntryPx, t.ExitPx, t.PctCh, t.PnLUSDT)
	}
	fmt.Println()
}

func SortEarlyTradesByEntry(trades []EarlyTrade) {
	sort.Slice(trades, func(i, j int) bool {
		if trades[i].EntryAt.Equal(trades[j].EntryAt) {
			return trades[i].Symbol < trades[j].Symbol
		}
		return trades[i].EntryAt.Before(trades[j].EntryAt)
	})
}

func FormatEarlySummary(s EarlyBacktestSummary) string {
	win := 0.0
	if s.Entries > 0 {
		win = float64(s.Wins) / float64(s.Entries) * 100
	}
	return fmt.Sprintf("signals=%d entries=%d pnl=%+.2f USDT win=%.1f%%", s.Signals, s.Entries, s.PnLUSDT, win)
}
