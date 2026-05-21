package whale

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"crypto_announcements_go/internal/binance"

	"gopkg.in/yaml.v3"
)

const (
	StrategyFlash    = "flash"
	StrategyBookLead = "booklead"
	StrategyBurst    = "burst"
)

type Config struct {
	Strategy string `yaml:"strategy"`

	Watchlist WatchlistConfig `yaml:"watchlist"`
	AllSymbols bool     `yaml:"all_symbols"`
	Symbols    []string `yaml:"symbols"`

	SymbolsPerConnection int `yaml:"symbols_per_connection"`
	DepthLevels          int `yaml:"depth_levels"`
	UpdateIntervalMs     int `yaml:"update_interval_ms"`

	Flash    FlashConfig    `yaml:"flash"`
	BookLead BookLeadConfig `yaml:"booklead"`
	Burst    BurstConfig    `yaml:"burst"`
	Risk      Risk         `yaml:"risk"`
	WebSocket WebSocketCfg `yaml:"websocket"`

	DryRun           bool    `yaml:"dry_run"`
	TradeLogPath     string  `yaml:"trade_log_path"` // append-only ENTRY/EXIT log (default whale-trades.log)
	DrySimMode       string  `yaml:"dry_sim_mode"`   // tick (aggTrade entry/exit) or mark (REST mark poll)
	DryEntrySlippageBps float64 `yaml:"dry_entry_slippage_bps"`
	DryExitSlippageBps  float64 `yaml:"dry_exit_slippage_bps"`
	CapitalUSDT      float64 `yaml:"capital_usdt"`
	UseLiveBalance   bool    `yaml:"use_live_balance"`
	// From .env when set: WHALE_ALLOCATION_PERCENT, WHALE_LEVERAGE (caps per symbol max).
	AllocationPercent float64 `yaml:"-"`
	Leverage            int     `yaml:"-"`
	CooldownSec      float64 `yaml:"cooldown_sec"`
	MaxOpenPositions int     `yaml:"max_open_positions"`
}

type WatchlistConfig struct {
	Mode          string   `yaml:"mode"` // custom, lowest_volume, mid_volume, all
	Size          int      `yaml:"size"`
	Skip          int      `yaml:"skip"`
	ExtraSymbols  []string `yaml:"extra_symbols"` // always monitored (e.g. AIGENSYNUSDT)
}

// FlashConfig detects large % moves in ~1 second from aggTrade flow (no announcements).
type FlashConfig struct {
	FastWindowMs         int     `yaml:"fast_window_ms"`          // 100 — entry timing
	SecWindowMs          int     `yaml:"sec_window_ms"`           // 1000 — measure 1s move
	MinSecMovePct        float64 `yaml:"min_sec_move_pct"`        // e.g. 5 — full flash
	EarlySecMovePct      float64 `yaml:"early_sec_move_pct"`      // e.g. 2 — building move
	MinFastMovePct       float64 `yaml:"min_fast_move_pct"`       // e.g. 0.8 — 100ms spike
	MaxSecMovePct        float64 `yaml:"max_sec_move_pct"`        // skip if already >12% in 1s
	MinSecNotionalUSDT   float64 `yaml:"min_sec_notional_usdt"`   // min $ in 1s
	MinFastNotionalUSDT  float64 `yaml:"min_fast_notional_usdt"`  // min $ in 100ms (early)
	SideDominancePct     float64 `yaml:"side_dominance_pct"`
}

// BurstConfig detects the start of violent ~1s moves from 100ms spike + volume.
type BurstConfig struct {
	FastWindowMs        int     `yaml:"fast_window_ms"`
	SecWindowMs         int     `yaml:"sec_window_ms"`
	MinFastMovePct      float64 `yaml:"min_fast_move_pct"`
	MinSecMovePct       float64 `yaml:"min_sec_move_pct"`
	MaxSecMovePct       float64 `yaml:"max_sec_move_pct"` // 0 = off
	MinFastNotionalUSDT float64 `yaml:"min_fast_notional_usdt"`
	MinSecNotionalUSDT  float64 `yaml:"min_sec_notional_usdt"`
	MaxSecNotionalUSDT  float64 `yaml:"max_sec_notional_usdt"` // pump_only: skip late fat 1s tape
	SideDominancePct    float64 `yaml:"side_dominance_pct"`
	SignalCooldownMs    int     `yaml:"signal_cooldown_ms"`
	// PumpOnly: stricter filters for violent pump starts (skip chop / counter-trend bursts).
	PumpOnly            bool    `yaml:"pump_only"`
	QuietBeforeMs       int     `yaml:"quiet_before_ms"`
	MaxQuietBeforeUSDT       float64 `yaml:"max_quiet_before_usdt"`        // fallback 5s cap
	MaxQuietBeforeUltraUSDT  float64 `yaml:"max_quiet_before_ultra_usdt"`  // MLN-style ultra flat
	MaxQuietBeforeFlatUSDT   float64 `yaml:"max_quiet_before_flat_usdt"`   // SYS-style; blocks normal 5s liq chop
	MinSecQuiet60Ratio       float64 `yaml:"min_sec_quiet60_ratio"`        // deprecated; use ultra/flat below
	MinSecQuiet60RatioUltra  float64 `yaml:"min_sec_quiet60_ratio_ultra"`  // sec/q60 MLN (low q60)
	MinSecQuiet60RatioFlat   float64 `yaml:"min_sec_quiet60_ratio_flat"`   // sec/q60 SYS (higher q60)
	MinBurstImpulseFlat      float64 `yaml:"min_burst_impulse_flat"`       // stricter sec/quiet5 when flat std
	MinVolumeAccel      float64 `yaml:"min_volume_accel"`
	MinFastVolSharePct  float64 `yaml:"min_fast_vol_share_pct"`
	MinBurstImpulse     float64 `yaml:"min_burst_impulse"`      // min sec$ / quiet$ before 1s leg
	MaxEntrySecMovePct  float64 `yaml:"max_entry_sec_move_pct"` // cap 1s move at entry (avoid chase)
	MaxFastMovePct      float64 `yaml:"max_fast_move_pct"`      // cap 100ms spike at entry
	MinMomentumAlign    float64 `yaml:"min_momentum_align"`     // min |1s%|/|100ms| same direction
	TrendWindowMs       int     `yaml:"trend_window_ms"`
	MaxCounterTrendPct  float64 `yaml:"max_counter_trend_pct"`
	// Cascade: violent multi-second leg (catches 13 May 13:30-style dumps when 1s spike is late).
	CascadeWindowMs       int     `yaml:"cascade_window_ms"`
	MinCascadeMovePct     float64 `yaml:"min_cascade_move_pct"`
	MinCascadeNotionalUSDT float64 `yaml:"min_cascade_notional_usdt"`
	MaxCascadeMovePct      float64 `yaml:"max_cascade_move_pct"` // skip if 3s leg already too extended
	// PreTrade: dead/flat tape before entry (mega profile from 13–14 May analysis).
	PreTradeEnabled         bool    `yaml:"pre_trade_enabled"`
	PreTradeMegaOnly        bool    `yaml:"pre_trade_mega_only"` // only flat-mega or elevated-mega tape profiles
	PreTradeWindowMs        int     `yaml:"pre_trade_window_ms"`
	MaxQuiet60sUSDT         float64 `yaml:"max_quiet_60s_usdt"`
	MaxQuiet30sUSDT         float64 `yaml:"max_quiet_30s_usdt"` // 0 = off; last N ms before burst (tighter chop cut)
	PreTradeShortWindowMs   int     `yaml:"pre_trade_short_window_ms"`
	MaxRange60sPct          float64 `yaml:"max_range_60s_pct"`
	MaxRange30sPct          float64 `yaml:"max_range_30s_pct"` // 0 = off
	MaxPrior1sMove60sPct    float64 `yaml:"max_prior_1s_move_60s_pct"`
	MaxTrades60s            int     `yaml:"max_trades_60s"` // 0 = off
	// Elevated-mega floor: separates AIGEN-style busy pump from 15 May chop (see pretrade diag).
	MinQuiet60ElevatedUSDT float64 `yaml:"min_quiet_60_elevated_usdt"`
	MinQuiet30ElevatedUSDT float64 `yaml:"min_quiet_30_elevated_usdt"`
	// Long pre-trade window (3h): dead tape before coordinated mega (10:30→13:30 style).
	PreTradeLongWindowMs int     `yaml:"pre_trade_long_window_ms"`
	MaxRangeLongPct      float64 `yaml:"max_range_long_pct"`
	MaxPrior1sLongPct    float64 `yaml:"max_prior_1s_long_pct"` // flat + ultra
	MinNotionalLongUSDT  float64 `yaml:"min_notional_long_usdt"`
	MaxQuiet30UltraUSDT  float64 `yaml:"max_quiet_30_ultra_usdt"`  // MLN-style (q30 ~$36)
	MaxQuiet60UltraUSDT  float64 `yaml:"max_quiet_60_ultra_usdt"`  // MLN q60 ~$225; STG fake ultra ~$519
	MinQuiet60FlatUSDT   float64 `yaml:"min_quiet_60_flat_usdt"`   // SYS ~$1808; PARTI chop ~$359 too thin
	MaxQuiet30FlatUSDT   float64 `yaml:"max_quiet_30_flat_usdt"`   // SYS-style (q30 ~$250)
	MaxQuiet60FlatUSDT   float64 `yaml:"max_quiet_60_flat_usdt"`   // cap 60s notional on flat mega (SYS ~$1808)
	MaxTrades30Flat      int     `yaml:"max_trades_30_flat"`       // fewer prints = dead tape (mega ≤8)
	MaxPrior1sLongElevatedPct float64 `yaml:"max_prior_1s_long_elevated_pct"`
}

// BookLeadConfig predicts violent moves from bid/ask depth + trade flow before price runs.
type BookLeadConfig struct {
	WindowMs              int     `yaml:"window_ms"`
	DepthLevels           int     `yaml:"depth_levels"`
	SweepBandPct          float64 `yaml:"sweep_band_pct"`
	MinImbalanceRatio     float64 `yaml:"min_imbalance_ratio"`
	MaxThinSideUSDT       float64 `yaml:"max_thin_side_usdt"`
	MinBookSideUSDT       float64 `yaml:"min_book_side_usdt"`
	MinTradeNotionalUSDT  float64 `yaml:"min_trade_notional_usdt"`
	TradeDominancePct     float64 `yaml:"trade_dominance_pct"`
	MaxEntryMovePct       float64 `yaml:"max_entry_move_pct"` // 0 = off (allow signals during violent moves)
	SignalCooldownMs      int     `yaml:"signal_cooldown_ms"`
}

type Risk struct {
	NormalRiskPercent      float64 `yaml:"normal_risk_percent"`
	MegaRiskPercent        float64 `yaml:"mega_risk_percent"`
	MaxPositionPercent     float64 `yaml:"max_position_percent"`
	StopLossPercent        float64 `yaml:"stop_loss_percent"`
	TakeProfitPercent1     float64 `yaml:"take_profit_percent_1"`
	TakeProfitPercent2     float64 `yaml:"take_profit_percent_2"`
	PartialExitFraction    float64 `yaml:"partial_exit_fraction"`
	MegaTrailActivatePct   float64 `yaml:"mega_trail_activate_pct"`
	MegaTrailDistancePct   float64 `yaml:"mega_trail_distance_pct"`
	MegaTakeProfitPct      float64 `yaml:"mega_take_profit_pct"`
	MegaStopLossPercent    float64 `yaml:"mega_stop_loss_percent"`
	MegaPartialMinPct      float64 `yaml:"mega_partial_min_pct"`       // partial only after this (not 3%)
	MegaTrailWidenPeakPct  float64 `yaml:"mega_trail_widen_peak_pct"`  // widen trail when peak exceeds
	MegaTrailWidenDistPct  float64 `yaml:"mega_trail_widen_dist_pct"`
	MegaTrailMinHoldMs     int     `yaml:"mega_trail_min_hold_ms"` // no trail exit in first N ms
	// Scratch chop: exit if peak favorable move < min within window (no mega follow-through).
	MegaConfirmWindowMs        int     `yaml:"mega_confirm_window_ms"`
	MegaConfirmMinFavorablePct float64 `yaml:"mega_confirm_min_favorable_pct"`
}

type WebSocketCfg struct {
	URL string `yaml:"url"`
}

func DefaultConfig() Config {
	return Config{
		Strategy: StrategyBurst,
		Watchlist: WatchlistConfig{
			Mode: "mid_volume",
			Size: 80,
			Skip: 200,
		},
		SymbolsPerConnection: 40,
		DepthLevels:          10,
		UpdateIntervalMs:     100,
		BookLead: BookLeadConfig{
			WindowMs:             1000,
			DepthLevels:          10,
			SweepBandPct:         3.0,
			MinImbalanceRatio:    1.2,
			MaxThinSideUSDT:      0,
			MinBookSideUSDT:      0,
			MinTradeNotionalUSDT: 5_000,
			TradeDominancePct:    52,
			MaxEntryMovePct:      0,
			SignalCooldownMs:     300,
		},
		Burst: BurstConfig{
			FastWindowMs:        100,
			SecWindowMs:         1000,
			MinFastMovePct:      0.5,
			MinSecMovePct:       0.4,
			MaxSecMovePct:       0,
			MinFastNotionalUSDT: 3_000,
			MinSecNotionalUSDT:  10_000,
			MaxSecNotionalUSDT:  45_000,
			SideDominancePct:    56,
			SignalCooldownMs:    400,
			PumpOnly:            true,
			QuietBeforeMs:       5_000,
			MaxQuietBeforeUSDT:  20_000,
			MinVolumeAccel:      3.0,
			MinFastVolSharePct:  30,
			MinBurstImpulse:     3.0,
			MaxEntrySecMovePct:  0.75,
			MaxFastMovePct:      0.58,
			MinMomentumAlign:    0.95,
			TrendWindowMs:       60_000,
			MaxCounterTrendPct:     1.5,
			CascadeWindowMs:          3_000,
			MinCascadeMovePct:        2.5,
			MinCascadeNotionalUSDT: 12_000,
			MaxCascadeMovePct:       4.0,
			PreTradeEnabled:         true,
			PreTradeMegaOnly:        true,
			PreTradeWindowMs:        60_000,
			PreTradeShortWindowMs:   30_000,
			MaxQuiet60sUSDT:         40_000,
			MaxQuiet30sUSDT:         14_000,
			MaxRange60sPct:          0.75,
			MaxRange30sPct:          0.42,
			MaxPrior1sMove60sPct:    0.28,
			MaxTrades60s:            280,
			MinQuiet60ElevatedUSDT:  35_000,
			MinQuiet30ElevatedUSDT:  12_000,
		},
		Flash: FlashConfig{
			FastWindowMs:        100,
			SecWindowMs:         1000,
			MinSecMovePct:       5.0,
			EarlySecMovePct:     2.0,
			MinFastMovePct:      0.8,
			MaxSecMovePct:       12.0,
			MinSecNotionalUSDT:  30_000,
			MinFastNotionalUSDT: 8_000,
			SideDominancePct:    55,
		},
		Risk: Risk{
			NormalRiskPercent:    2.0,
			MegaRiskPercent:      5.0,
			MaxPositionPercent:   5.0,
			StopLossPercent:      1.5,
			TakeProfitPercent1:   3.0,
			TakeProfitPercent2:   6.0,
			PartialExitFraction:  0.5,
			MegaTrailActivatePct: 2.0,
			MegaTrailDistancePct: 2.0,
			MegaTakeProfitPct:       12.0,
			MegaStopLossPercent:     2.0,
			MegaPartialMinPct:       5.0,
			MegaTrailWidenPeakPct:   8.0,
			MegaTrailWidenDistPct:   3.5,
			MegaTrailMinHoldMs:      2000,
		},
		WebSocket:        WebSocketCfg{URL: "wss://fstream.binance.com"},
		DryRun:           true,
		CapitalUSDT:      1000,
		CooldownSec:      1800,
		MaxOpenPositions: 2,
	}
}

func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		path = os.Getenv("WHALE_CONFIG")
	}
	if path == "" {
		path = "config/whale.yaml"
	}
	if b, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(b, &cfg); err != nil {
			return cfg, err
		}
	}
	applyEnv(&cfg)
	cfg.normalize()
	return cfg, nil
}

func applyEnv(c *Config) {
	if v := os.Getenv("WHALE_DRY_RUN"); v != "" {
		c.DryRun = strings.EqualFold(v, "true") || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("WHALE_TRADE_LOG")); v != "" {
		c.TradeLogPath = v
	}
	if v := strings.TrimSpace(os.Getenv("WHALE_CAPITAL_USDT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.CapitalUSDT = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("WHALE_ALLOCATION_PERCENT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.AllocationPercent = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("WHALE_LEVERAGE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Leverage = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("WHALE_DRY_SIM_MODE")); v != "" {
		c.DrySimMode = strings.ToLower(v)
	}
}

func (c *Config) UsesTickDrySim() bool {
	return !strings.EqualFold(strings.TrimSpace(c.DrySimMode), "mark")
}

func (c *Config) normalize() {
	if c.Strategy == "" {
		c.Strategy = StrategyBurst
	}
	b := &c.Burst
	if b.FastWindowMs <= 0 {
		b.FastWindowMs = 100
	}
	if b.SecWindowMs <= 0 {
		b.SecWindowMs = 1000
	}
	if b.MinFastMovePct <= 0 {
		b.MinFastMovePct = 0.5
	}
	if b.MinFastNotionalUSDT <= 0 {
		b.MinFastNotionalUSDT = 3_000
	}
	if b.MinSecNotionalUSDT <= 0 {
		b.MinSecNotionalUSDT = 8_000
	}
	if b.SideDominancePct <= 0 {
		b.SideDominancePct = 52
	}
	if b.SignalCooldownMs <= 0 {
		b.SignalCooldownMs = 400
	}
	if c.Risk.MegaTrailActivatePct <= 0 {
		c.Risk.MegaTrailActivatePct = 2.0
	}
	if c.Risk.MegaTrailDistancePct <= 0 {
		c.Risk.MegaTrailDistancePct = 2.0
	}
	if c.Risk.MegaTakeProfitPct <= 0 {
		c.Risk.MegaTakeProfitPct = 12.0
	}
	if c.Risk.MegaStopLossPercent <= 0 {
		c.Risk.MegaStopLossPercent = 2.0
	}
	if c.Watchlist.Size <= 0 {
		c.Watchlist.Size = 80
	}
	if c.DepthLevels <= 0 {
		c.DepthLevels = 10
	}
	if c.UpdateIntervalMs <= 0 {
		c.UpdateIntervalMs = 100
	}
	bl := &c.BookLead
	if bl.WindowMs <= 0 {
		bl.WindowMs = 1000
	}
	if bl.DepthLevels <= 0 {
		bl.DepthLevels = c.DepthLevels
	}
	if bl.SweepBandPct <= 0 {
		bl.SweepBandPct = 3.0
	}
	// MinImbalanceRatio, MaxThinSideUSDT, MinBookSideUSDT: 0 = disabled (violent-move mode).
	if bl.MinTradeNotionalUSDT <= 0 {
		bl.MinTradeNotionalUSDT = 5_000
	}
	if bl.TradeDominancePct <= 0 {
		bl.TradeDominancePct = 52
	}
	if c.Flash.FastWindowMs <= 0 {
		c.Flash.FastWindowMs = 100
	}
	if c.Flash.SecWindowMs <= 0 {
		c.Flash.SecWindowMs = 1000
	}
	if c.Flash.MinSecMovePct <= 0 {
		c.Flash.MinSecMovePct = 5.0
	}
	if c.Flash.EarlySecMovePct <= 0 {
		c.Flash.EarlySecMovePct = 2.0
	}
	if c.Flash.MinFastMovePct <= 0 {
		c.Flash.MinFastMovePct = 0.8
	}
	if c.Flash.MaxSecMovePct <= 0 {
		c.Flash.MaxSecMovePct = 12.0
	}
	if c.Flash.SideDominancePct <= 0 {
		c.Flash.SideDominancePct = 55
	}
	if c.CooldownSec <= 0 {
		c.CooldownSec = 60
	}
	if c.MaxOpenPositions <= 0 {
		c.MaxOpenPositions = 2
	}
	if strings.TrimSpace(c.DrySimMode) == "" {
		c.DrySimMode = "tick"
	}
	if c.SymbolsPerConnection <= 0 {
		c.SymbolsPerConnection = 80
	}
	if c.Risk.PartialExitFraction <= 0 || c.Risk.PartialExitFraction > 1 {
		c.Risk.PartialExitFraction = 0.5
	}
	for i := range c.Symbols {
		c.Symbols[i] = strings.ToUpper(strings.TrimSpace(c.Symbols[i]))
	}
	for i := range c.Watchlist.ExtraSymbols {
		c.Watchlist.ExtraSymbols[i] = strings.ToUpper(strings.TrimSpace(c.Watchlist.ExtraSymbols[i]))
	}
}

func (c *Config) ResolveWatchlist(client *binance.FuturesClient, perps []string) error {
	return resolveWatchlist(c, client, perps)
}

func (c *Config) TuneForSymbolCount() {
	n := len(c.Symbols)
	maxPerConn := 80
	if c.UsesBurst() || c.UsesFlash() {
		maxPerConn = 80
	} else if c.UsesBookLead() {
		maxPerConn = 40 // depth+aggTrade = 2 streams per symbol
		if c.SymbolsPerConnection <= 0 || c.SymbolsPerConnection > maxPerConn {
			c.SymbolsPerConnection = maxPerConn
		}
	}
	if n <= maxPerConn {
		return
	}
	if c.SymbolsPerConnection > maxPerConn {
		c.SymbolsPerConnection = maxPerConn
	}
}

func (c *Config) Cooldown() time.Duration {
	return time.Duration(c.CooldownSec * float64(time.Second))
}

func (c *Config) UsesFlash() bool {
	return strings.EqualFold(c.Strategy, StrategyFlash)
}

func (c *Config) UsesBurst() bool {
	s := strings.ToLower(strings.TrimSpace(c.Strategy))
	return s == StrategyBurst
}

func (c *Config) UsesBookLead() bool {
	s := strings.ToLower(strings.TrimSpace(c.Strategy))
	return s == StrategyBookLead || s == "book_lead" || s == "book"
}

func FormatStreams(cfg Config) string {
	if cfg.UsesBurst() || cfg.UsesFlash() {
		return fmt.Sprintf("burst/flash aggTrade (%d symbols)", len(cfg.Symbols))
	}
	if cfg.UsesBookLead() {
		levels := cfg.DepthLevels
		if cfg.BookLead.DepthLevels > 0 {
			levels = cfg.BookLead.DepthLevels
		}
		ms := cfg.UpdateIntervalMs
		return fmt.Sprintf("booklead depth%d@%dms+aggTrade (%d symbols)", levels, ms, len(cfg.Symbols))
	}
	return fmt.Sprintf("flash aggTrade (%d symbols)", len(cfg.Symbols))
}
