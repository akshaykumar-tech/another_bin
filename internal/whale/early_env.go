package whale

import (
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// ApplyEarlyEnv overlays EARLY_* variables (used by cmd/whale-early).
func ApplyEarlyEnv(c *Config) {
	if v := strings.TrimSpace(os.Getenv("EARLY_DRY_RUN")); v != "" {
		c.DryRun = strings.EqualFold(v, "true") || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_LIVE_TRADE")); v != "" {
		c.EarlyLiveTrade = strings.EqualFold(v, "true") || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_MARGIN_USDT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.MarginUSDT = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_ALLOCATION_PERCENT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.AllocationPercent = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_LEVERAGE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Leverage = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_MAX_LEVERAGE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MaxLeverageCap = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_CAPITAL_USDT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.CapitalUSDT = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_TRADE_LOG")); v != "" {
		c.TradeLogPath = v
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_FOCUS_COOLDOWN_SEC")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.FocusCooldownSec = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_RULE")); v != "" {
		c.Early.Rule = strings.ToLower(v)
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_DIRECTION")); v != "" {
		c.Early.Direction = strings.ToLower(v)
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_DRY_SAME")); v != "" {
		c.Early.DrySameEnabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_DRY_REVERSE_LIMIT")); v != "" {
		c.Early.DryReverseLimitEnabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_LIVE_REVERSE_LIMIT")); v != "" {
		c.Early.LiveReverseLimitEnabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_MIN_VOL_ACCEL")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.Early.MinVolAccel = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_LIMIT_FILL_SEC")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.Early.LiveLimitFillSec = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_TAKE_PROFIT_PCT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 0 {
			c.Early.TakeProfitPct = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_SAME_MARGIN_USDT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.EarlySameMarginUSDT = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_SAME_ALLOCATION_PERCENT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.EarlySameAllocationPercent = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_SAME_LEVERAGE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.EarlySameLeverage = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_REVERSE_MARGIN_USDT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.EarlyReverseMarginUSDT = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_REVERSE_ALLOCATION_PERCENT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.EarlyReverseAllocationPercent = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_REVERSE_LEVERAGE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.EarlyReverseLeverage = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_WATCHLIST_SIZE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Watchlist.Size = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_WATCHLIST_MODE")); v != "" {
		c.Watchlist.Mode = strings.ToLower(v)
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_NOTIONAL_USDT")); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n > 0 {
			c.EarlyNotionalUSDT = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("EARLY_MAX_OPEN_LIVE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.EarlyMaxOpenLive = n
		}
	}
}

// LoadEarlyConfig loads whale-early.yaml + EARLY_* env.
func LoadEarlyConfig(path string) (Config, error) {
	if path == "" {
		path = "config/whale-early.yaml"
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		return cfg, err
	}
	applyEarlyFileOverlay(path, &cfg)
	ApplyEarlyEnv(&cfg)
	cfg.Strategy = StrategyEarly
	cfg.Early.ApplyDefaults()
	if cfg.DryRun && os.Getenv("EARLY_DRY_SAME") == "" && !cfg.Early.DryReverseLimitEnabled {
		cfg.Early.DrySameEnabled = true
	}
	if cfg.CooldownSec <= 0 {
		cfg.CooldownSec = float64(cfg.Early.HoldMinutes * 60)
	}
	if cfg.FocusCooldownSec <= 0 {
		cfg.FocusCooldownSec = 120
	}
	if cfg.MaxOpenPositions <= 0 {
		cfg.MaxOpenPositions = 3
	}
	if cfg.TradeLogPath == "" {
		cfg.TradeLogPath = "early-trades.log"
	}
	if cfg.EarlyMaxOpenLive <= 0 {
		cfg.EarlyMaxOpenLive = 3
	}
	return cfg, nil
}

type earlyFileOverlay struct {
	MarginUSDT float64 `yaml:"margin_usdt"`
	Leverage   int     `yaml:"leverage"`
	DryRun     *bool   `yaml:"dry_run"`
	EarlyLive  *bool   `yaml:"early_live_trade"`
}

func applyEarlyFileOverlay(path string, c *Config) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var o earlyFileOverlay
	if yaml.Unmarshal(b, &o) != nil {
		return
	}
	if c.MarginUSDT <= 0 && o.MarginUSDT > 0 {
		c.MarginUSDT = o.MarginUSDT
	}
	if c.Leverage <= 0 && o.Leverage > 0 {
		c.Leverage = o.Leverage
	}
	if o.DryRun != nil && os.Getenv("EARLY_DRY_RUN") == "" {
		c.DryRun = *o.DryRun
	}
	if o.EarlyLive != nil && os.Getenv("EARLY_LIVE_TRADE") == "" {
		c.EarlyLiveTrade = *o.EarlyLive
	}
}
