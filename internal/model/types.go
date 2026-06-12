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

// FuturesLotRules holds exchange filters for order formatting (Binance USDT-M).
type FuturesLotRules struct {
	PriceTickSize  string
	PricePrecision int
	StepSize       float64 // LOT_SIZE step
	MinQty         float64
	MinNotional    float64 // MIN_NOTIONAL / notional floor
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
