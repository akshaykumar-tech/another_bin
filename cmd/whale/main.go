package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/config"
	"crypto_announcements_go/internal/whale"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	cfgPath := flag.String("config", "", "path to whale.yaml")
	flag.Parse()

	_ = godotenv.Load()
	appCfg := config.Load()

	whaleCfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("whale config: %v", err)
	}
	if v := strings.TrimSpace(os.Getenv("WHALE_DRY_RUN")); v != "" {
		whaleCfg.DryRun = v == "1" || strings.EqualFold(v, "true")
	}

	client := binance.NewFuturesClient("https://fapi.binance.com", appCfg.BinanceAPIKey, appCfg.BinanceAPISecret)
	if err := client.WarmSymbolCache(); err != nil {
		log.Fatalf("[whale] exchangeInfo: %v", err)
	}
	perps := client.USDTPerpetualSymbols()
	if err := whaleCfg.ResolveWatchlist(client, perps); err != nil {
		log.Fatalf("[whale] watchlist: %v", err)
	}
	whaleCfg.TuneForSymbolCount()
	if len(whaleCfg.Symbols) == 0 {
		log.Fatal("[whale] no symbols")
	}
	logWatchlistHints(whaleCfg.Symbols)
	logWatchlistSymbols(whaleCfg.Symbols)

	alloc := whaleCfg.Risk.MegaRiskPercent
	if whaleCfg.AllocationPercent > 0 {
		alloc = whaleCfg.AllocationPercent
	}
	lev := whaleCfg.Leverage
	if lev <= 0 {
		lev = 1
	}
	log.Printf("[whale] starting %s dry_run=%v dry_sim=%s capital=%.0f alloc=%.1f%% lev=%dx %s",
		whaleCfg.Strategy, whaleCfg.DryRun, whaleCfg.DrySimMode, whaleCfg.CapitalUSDT, alloc, lev,
		whale.FormatStreams(whaleCfg))

	runner, err := whale.NewRunner(whaleCfg, client)
	if err != nil {
		log.Fatalf("[whale] journal: %v", err)
	}
	if p := runner.TradeLogPath(); p != "" {
		log.Printf("[whale] trade journal: %s (cat this file for ENTRY/EXIT PnL)", p)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[whale] runner exit: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[whale] shutdown")
	time.Sleep(200 * time.Millisecond)
}

// logWatchlistSymbols prints every subscribed symbol once at startup.
func logWatchlistSymbols(symbols []string) {
	const cols = 5
	log.Printf("[whale] watchlist tokens (%d):", len(symbols))
	var line strings.Builder
	for i, sym := range symbols {
		if i%cols == 0 {
			if i > 0 {
				log.Print(line.String())
			}
			line.Reset()
			line.WriteString("[whale]   ")
		} else {
			line.WriteString("  ")
		}
		line.WriteString(sym)
	}
	if line.Len() > 0 {
		log.Print(line.String())
	}
}

// logWatchlistHints warns when common backtest symbols are not subscribed live.
func logWatchlistHints(symbols []string) {
	have := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		have[strings.ToUpper(s)] = struct{}{}
	}
	for _, sym := range []string{"MLNUSDT", "SYSUSDT", "ATAUSDT", "PHBUSDT"} {
		if _, ok := have[sym]; !ok {
			log.Printf("[whale] watchlist: %s NOT monitored — add to watchlist.extra_symbols to match replay", sym)
		}
	}
}
