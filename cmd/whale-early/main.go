// Early watch live bot: quiet_flat / dead_tape signals, dry-run and/or live Binance orders.
//
//	go run ./cmd/whale-early/ -config config/whale-early.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	cfgPath := flag.String("config", "config/whale-early.yaml", "")
	flag.Parse()

	_ = godotenv.Load()

	cfg, err := whale.LoadEarlyConfig(*cfgPath)
	if err != nil {
		log.Fatalf("[early] config: %v", err)
	}

	if !cfg.DryRun && !cfg.EarlyLiveTrade {
		log.Fatal("[early] set EARLY_DRY_RUN=true and/or EARLY_LIVE_TRADE=true")
	}
	if cfg.DryRun && !cfg.Early.DrySameEnabled && !cfg.Early.DryReverseLimitEnabled {
		log.Fatal("[early] dry run requires EARLY_DRY_SAME=true and/or EARLY_DRY_REVERSE_LIMIT=true")
	}

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	if err := client.WarmSymbolCache(); err != nil {
		log.Fatalf("[early] exchangeInfo: %v", err)
	}
	if cfg.EarlyLiveTrade {
		if !client.Configured() {
			log.Fatal("[early] EARLY_LIVE_TRADE requires BINANCE_API_KEY and BINANCE_API_SECRET")
		}
		if cfg.MarginUSDT <= 0 && cfg.AllocationPercent <= 0 {
			log.Fatal("[early] live trade requires EARLY_MARGIN_USDT or EARLY_ALLOCATION_PERCENT")
		}
	}

	perps := client.USDTPerpetualSymbols()
	if err := cfg.ResolveWatchlist(client, perps); err != nil {
		log.Fatalf("[early] watchlist: %v", err)
	}
	cfg.TuneForSymbolCount()
	if len(cfg.Symbols) == 0 {
		log.Fatal("[early] no symbols")
	}

	log.Printf("[early] starting | dry_run=%v live_trade=%v rule=%s direction=%s | symbols=%d",
		cfg.DryRun, cfg.EarlyLiveTrade, cfg.Early.Rule, cfg.Early.Direction, len(cfg.Symbols))
	log.Printf("[early] dry_same=%v dry_rev_limit=%v min_vol=%.1fx limit_fill=%ds",
		cfg.Early.DrySameEnabled, cfg.Early.DryReverseLimitEnabled, cfg.Early.MinVolAccel, int(cfg.Early.LiveLimitFillSec))
	log.Printf("[early] margin=%s leverage=%d hold=%dm scan=%dm log=%s",
		marginDesc(cfg), cfg.Leverage, cfg.Early.HoldMinutes, cfg.Early.ScanStepMinutes, cfg.TradeLogPath)

	runner, err := whale.NewEarlyRunner(cfg, client)
	if err != nil {
		log.Fatalf("[early] runner: %v", err)
	}
	if p := runner.TradeLogPath(); p != "" {
		log.Printf("[early] trade journal: %s", p)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[early] runner exit: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[early] shutdown")
	time.Sleep(200 * time.Millisecond)
}

func marginDesc(c whale.Config) string {
	if c.MarginUSDT > 0 {
		return fmt.Sprintf("%.2f USDT fixed", c.MarginUSDT)
	}
	if c.AllocationPercent > 0 {
		return fmt.Sprintf("%.0f%% balance", c.AllocationPercent)
	}
	return fmt.Sprintf("%.0f USDT sim", c.CapitalUSDT)
}
