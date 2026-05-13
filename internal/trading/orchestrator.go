package trading

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"crypto_announcements_go/internal/model"
	"crypto_announcements_go/internal/repo"
)

type FuturesClient interface {
	RecentMovePercent(symbol string, lookbackSec int) (float64, error)
	MaxMoveInWindow(symbol string, lookbackSec int) (maxUp, maxDown float64, err error)
	SymbolTradable(symbol string) bool
	MarketOrder(symbol, side string, qty float64) (map[string]any, error)
	Configured() bool
	MarkPrice(symbol string) (float64, error)
	LotRules(symbol string) (model.FuturesLotRules, error)
	StopMarketCloseFull(symbol, side, stopPriceStr, workingType string) (map[string]any, error)
}

type Orchestrator struct {
	repo               *repo.Repo
	client             FuturesClient
	recentMoveEnabled  bool
	recentMoveLookback int
	recentMoveSkipPct  float64
	fixedMarginUSDT    float64
}

func New(r *repo.Repo, c FuturesClient, recentMoveEnabled bool, recentMoveLookback int, recentMoveSkipPct, fixedMarginUSDT float64) *Orchestrator {
	return &Orchestrator{
		repo:               r,
		client:             c,
		recentMoveEnabled:  recentMoveEnabled,
		recentMoveLookback: recentMoveLookback,
		recentMoveSkipPct:  recentMoveSkipPct,
		fixedMarginUSDT:    fixedMarginUSDT,
	}
}

func (o *Orchestrator) HandleAnnouncement(ctx context.Context, a model.Announcement) error {
	setting, err := o.repo.TradingSetting(ctx)
	if err != nil {
		return err
	}
	if !setting.Enabled {
		return nil
	}

	action := setting.ActionFor(a.AnnouncementType)
	if action == "" || action == "none" {
		return nil
	}

	active, err := o.repo.OpenTradeCount(ctx)
	if err != nil {
		return err
	}
	if active >= setting.MaxTokensToTrade {
		return nil
	}

	remaining := setting.MaxTokensToTrade - active
	tokens := a.AffectedTokens
	if len(tokens) == 0 {
		return nil
	}
	if len(tokens) > remaining {
		tokens = tokens[:remaining]
	}

	// Fire all token orders in parallel for minimum latency.
	var wg sync.WaitGroup
	for _, base := range tokens {
		base = strings.ToUpper(strings.TrimSpace(base))
		if base == "" {
			continue
		}
		symbol := base + "USDT"
		posSide := "long"
		orderSide := "BUY"
		if action == "open_short" {
			posSide = "short"
			orderSide = "SELL"
		}

		if !o.client.SymbolTradable(symbol) {
			_ = o.repo.InsertTradeExecutionFailed(ctx, a.ID, symbol, base, posSide, "Unknown or inactive symbol "+symbol, map[string]any{
				"order_error": "Unknown or inactive symbol " + symbol,
			})
			continue
		}

		wg.Add(1)
		go func(symbol, base, posSide, orderSide string) {
			defer wg.Done()
			qty := o.estimateQty()
			resp, err := o.client.MarketOrder(symbol, orderSide, qty)
			if err != nil {
				_ = o.repo.InsertTradeExecutionFailed(ctx, a.ID, symbol, base, posSide, err.Error(), map[string]any{"order_error": err.Error()})
				log.Printf("[trading] %s order failed: %v", symbol, err)
				return
			}
			raw := cloneMapAny(resp)
			fillAvg := parseEntryPrice(resp)
			entry := fillAvg
			if entry <= 0 && o.client.Configured() {
				if mp, err := o.client.MarkPrice(symbol); err == nil && mp > 0 {
					entry = mp
				}
			}

			// Stop-loss in background — don't block the order confirmation path.
			go func() {
				o.mergeStopLossIntoRaw(raw, setting, symbol, posSide, entry, fillAvg > 0)
			}()

			orderID, _ := resp["clientOrderId"].(string)
			if err := o.repo.InsertTradeExecutionOpen(ctx, a.ID, symbol, base, posSide, qty, entry, orderID, raw); err != nil {
				log.Printf("[trading] %s db insert failed: %v", symbol, err)
			}
		}(symbol, base, posSide, orderSide)
	}
	wg.Wait()
	return nil
}

// mergeStopLossIntoRaw mirrors Rails merge_stop_loss_into_execution!.
// hasExchangeFill: true when the entry order returned a real avg price (required to submit STOP_MARKET closePosition).
func (o *Orchestrator) mergeStopLossIntoRaw(raw map[string]any, setting model.TradingSetting, symbol, posSide string, entry float64, hasExchangeFill bool) {
	if !setting.StopLossEnabled || setting.StopLossPercent <= 0 {
		return
	}
	if !o.client.Configured() {
		return
	}
	if entry <= 0 {
		return
	}
	rules, err := o.client.LotRules(symbol)
	if err != nil {
		raw["stop_loss"] = map[string]any{
			"order_error": err.Error(),
		}
		return
	}
	stopStr, closeSide, meta, ok := ComputeStopLossPlan(entry, setting.StopLossPercent, posSide, rules)
	if !ok {
		return
	}
	if setting.DryRun {
		raw["stop_loss"] = meta
		return
	}
	if !hasExchangeFill {
		// Same SL math as Rails, but do not hit the exchange without a real position/fill (stub MARKET orders).
		raw["stop_loss"] = map[string]any{
			"planned":               meta,
			"exchange_submit":       false,
			"reason":                "no_fill_avg_price",
			"note":                  "Replace MarketOrder stub with a signed fill to submit STOP_MARKET to Binance.",
		}
		return
	}
	slResp, err := o.client.StopMarketCloseFull(symbol, closeSide, stopStr, "MARK_PRICE")
	if err != nil {
		raw["stop_loss"] = map[string]any{
			"order_error": err.Error(),
		}
		return
	}
	merged := map[string]any{
		"stop_loss_percent": meta["stop_loss_percent"],
		"stop_loss_price":   meta["stop_loss_price"],
		"stop_loss_side":    meta["stop_loss_side"],
		"working_type":      meta["working_type"],
		"order_response":    slResp,
	}
	raw["stop_loss"] = merged
}

func parseEntryPrice(resp map[string]any) float64 {
	if v, ok := resp["avgPrice"].(float64); ok && v > 0 {
		return v
	}
	if s, ok := resp["avgPrice"].(string); ok && s != "" {
		var x float64
		_, _ = fmt.Sscanf(s, "%f", &x)
		return x
	}
	return 0
}

func cloneMapAny(src map[string]any) map[string]any {
	if src == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func (o *Orchestrator) estimateQty() float64 {
	// Keep fixed and deterministic in ultra-fast path.
	if o.fixedMarginUSDT <= 0 {
		return 1
	}
	return o.fixedMarginUSDT
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
