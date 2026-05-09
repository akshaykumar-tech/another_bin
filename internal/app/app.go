package app

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/config"
	"crypto_announcements_go/internal/db"
	"crypto_announcements_go/internal/repo"
	"crypto_announcements_go/internal/trading"
	"crypto_announcements_go/internal/upbit"
)

type App struct {
	cfg      config.Config
	repo     *repo.Repo
	binance  *binance.AnnouncementStream
	upbit    *upbit.Fetcher
}

func New(cfg config.Config) (*App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	r := repo.New(pool)
	futures := binance.NewFuturesClient("https://fapi.binance.com", cfg.BinanceAPIKey, cfg.BinanceAPISecret)
	_ = futures.WarmSymbolCache()
	tr := trading.New(r, futures, cfg.RecentMoveFilterEnabled, cfg.RecentMoveLookbackSec, cfg.RecentMoveSkipPercent, cfg.UltraFastFixedMargin)
	bws := binance.NewAnnouncementStream(cfg.BinanceWSBaseURL, cfg.BinanceWSTopic, cfg.BinanceAPIKey, cfg.BinanceAPISecret, r, tr)
	uf := upbit.New(cfg.UpbitAPIURL, cfg.UpbitPerPage, cfg.UpbitOnlyLatest, r, tr)
	return &App{cfg: cfg, repo: r, binance: bws, upbit: uf}, nil
}

func (a *App) Run(ctx context.Context) error {
	mode := strings.ToLower(a.cfg.ServiceMode)
	errCh := make(chan error, 2)

	if mode == "all" || mode == "binance" {
		if !a.cfg.BinanceWSEnabled {
			log.Printf("[app] binance ws disabled by env")
		} else {
			go func() { errCh <- a.binance.Run(ctx) }()
		}
	}
	if mode == "all" || mode == "upbit" {
		go func() {
			t := time.NewTicker(a.cfg.UpbitPollInterval)
			defer t.Stop()
			for {
				if err := a.upbit.Poll(ctx); err != nil {
					log.Printf("[upbit] poll error: %v", err)
				}
				select {
				case <-ctx.Done():
					errCh <- nil
					return
				case <-t.C:
				}
			}
		}()
	}

	for i := 0; i < cap(errCh); i++ {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		}
	}
	return nil
}
