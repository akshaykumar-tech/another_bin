package whale

import (
	"context"
	"time"

	"crypto_announcements_go/internal/binance"
)

type BacktestOptions struct {
	Start   time.Time
	End     time.Time
	Verbose bool
}

type BacktestSummary struct {
	Symbols            int
	TradesLoaded       int
	Signals            int
	Entries            int
	SkippedMaxPos      int
	SkippedCooldown    int
	SkippedNotTradable int
	SkippedRequireAnn  int
	ExitsSL            int
	ExitsTP1           int
	ExitsTP2           int
	ExitsTimeout       int
	TotalPnLUSDT       float64
}

func RunBacktest(ctx context.Context, cfg Config, client *binance.FuturesClient, opt BacktestOptions) (BacktestSummary, error) {
	if opt.End.IsZero() {
		opt.End = time.Now().UTC()
	}
	if opt.Start.IsZero() {
		opt.Start = opt.End.Add(-24 * time.Hour)
	}
	ds, err := loadMarketDataset(ctx, cfg, client, opt.Start, opt.End)
	if err != nil {
		return BacktestSummary{}, err
	}
	return RunBacktestOnDataset(cfg, client, ds, opt.Verbose), nil
}

func RunBacktestOnDataset(cfg Config, client *binance.FuturesClient, ds *marketDataset, verbose bool) BacktestSummary {
	st := newReplayState(cfg, client, verbose)
	return st.replay(ds)
}
