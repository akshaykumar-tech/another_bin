package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL string

	ServiceMode string

	BinanceWSBaseURL string
	BinanceWSTopic   string
	BinanceWSEnabled bool
	BinanceAPIKey    string
	BinanceAPISecret string

	UpbitPollInterval time.Duration
	UpbitAPIURL       string
	UpbitPerPage      int
	UpbitOnlyLatest   bool
	// UpbitLogSkippedUnclassified logs each API notice we skip (no classify rule); default off (noisy every poll).
	UpbitLogSkippedUnclassified bool

	TradeEnabled            bool
	TradeDryRun             bool
	RecentMoveFilterEnabled bool
	RecentMoveLookbackSec   int
	RecentMoveSkipPercent   float64
	UltraFastFixedMargin    float64
}

func Load() Config {
	return Config{
		DatabaseURL: must("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/crypto_announcements_development?sslmode=disable"),
		ServiceMode: lower(must("ANNOUNCEMENT_SERVICE_MODE", "all")),

		BinanceWSBaseURL: must("BINANCE_CMS_WS_BASE_URL", "wss://api.binance.com/sapi/wss"),
		BinanceWSTopic:   must("BINANCE_CMS_TOPIC", "com_announcement_en"),
		BinanceWSEnabled: mustBool("BINANCE_WS_ENABLED", true),
		BinanceAPIKey:    must("BINANCE_API_KEY", ""),
		BinanceAPISecret: must("BINANCE_API_SECRET", ""),

		UpbitPollInterval: time.Duration(mustInt("UPBIT_POLL_INTERVAL_SECONDS", 5)) * time.Second,
		UpbitAPIURL:       must("UPBIT_ANNOUNCEMENTS_API_URL", "https://api-manager.upbit.com/api/v1/announcements"),
		UpbitPerPage:      mustInt("UPBIT_ANNOUNCEMENTS_API_PER_PAGE", 1),
		UpbitOnlyLatest:   mustBool("UPBIT_ANNOUNCEMENTS_ONLY_LATEST", true),
		UpbitLogSkippedUnclassified: mustBool("UPBIT_LOG_SKIPPED_UNCLASSIFIED", false),

		TradeEnabled:            mustBool("AUTO_TRADING_ENABLED", true),
		TradeDryRun:             mustBool("AUTO_TRADING_DRY_RUN", false),
		RecentMoveFilterEnabled: mustBool("AUTO_TRADING_RECENT_MOVE_FILTER_ENABLED", true),
		RecentMoveLookbackSec:   mustInt("AUTO_TRADING_RECENT_MOVE_LOOKBACK_SECONDS", 20),
		RecentMoveSkipPercent:   mustFloat("AUTO_TRADING_RECENT_MOVE_SKIP_PERCENT", 10),
		UltraFastFixedMargin:    mustFloat("AUTO_TRADING_ULTRA_FAST_FIXED_MARGIN_USDT", 20),
	}
}

func must(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func mustInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func mustFloat(k string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func mustBool(k string, def bool) bool {
	v := lower(strings.TrimSpace(os.Getenv(k)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func lower(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
