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
