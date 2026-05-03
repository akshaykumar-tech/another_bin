package model

import "time"

type Exchange struct {
	ID   int64
	Code string
}

type Announcement struct {
	ID                int64
	ExchangeID        int64
	Title             string
	Content           string
	AnnouncementType  string
	Severity          string
	PublishedAt       time.Time
	AffectedTokens    []string
	RecommendedAction string
	RawData           []byte
}

type TradingSetting struct {
	Enabled                bool
	DryRun                 bool
	Leverage               int
	AllocationPercent      float64
	MaxTokensToTrade       int
	StopLossEnabled        bool
	StopLossPercent        float64
	AnnouncementActionsRaw map[string]string
}

// FuturesLotRules holds PRICE_FILTER fields used to format stop prices (Binance USDT-M).
type FuturesLotRules struct {
	PriceTickSize  string
	PricePrecision int
}

func (t TradingSetting) ActionFor(typ string) string {
	if t.AnnouncementActionsRaw == nil {
		return "none"
	}
	v := t.AnnouncementActionsRaw[typ]
	if v == "" {
		return "none"
	}
	return v
}
