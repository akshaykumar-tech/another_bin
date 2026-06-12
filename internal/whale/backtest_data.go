package whale

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"crypto_announcements_go/internal/binance"
)

type replayTrade struct {
	Symbol       string
	Time         time.Time
	Price        float64
	Quantity     float64
	BuyerIsMaker bool
}

type marketDataset struct {
	trades []replayTrade
	bySym  map[string][]int // indices into trades sorted by time
}

func loadMarketDataset(ctx context.Context, cfg Config, client *binance.FuturesClient, start, end time.Time) (*marketDataset, error) {
	ds := &marketDataset{bySym: make(map[string][]int)}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	errCh := make(chan error, 1)

	for i, sym := range cfg.Symbols {
		sym := sym
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			rows, err := client.FetchAggTradesRange(sym, start, end)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", sym, err):
				default:
				}
				return
			}
			mu.Lock()
			base := len(ds.trades)
			for j, r := range rows {
				ds.trades = append(ds.trades, replayTrade{
					Symbol: sym, Time: r.Time, Price: r.Price, Quantity: r.Quantity, BuyerIsMaker: r.BuyerIsMaker,
				})
				ds.bySym[sym] = append(ds.bySym[sym], base+j)
			}
			mu.Unlock()
			if (idx+1)%20 == 0 || idx+1 == len(cfg.Symbols) {
				log.Printf("[backtest] loaded %d/%d symbols", idx+1, len(cfg.Symbols))
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	sort.Slice(ds.trades, func(i, j int) bool {
		if ds.trades[i].Time.Equal(ds.trades[j].Time) {
			return ds.trades[i].Symbol < ds.trades[j].Symbol
		}
		return ds.trades[i].Time.Before(ds.trades[j].Time)
	})
	return ds, nil
}
