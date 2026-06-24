package whale

import "time"

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type SignalKind string

const (
	SignalFlash    SignalKind = "flash"
	SignalBookLead SignalKind = "booklead"
	SignalBurst    SignalKind = "burst"
)

type Signal struct {
	Symbol      string
	Side        Side
	Kind        SignalKind
	MovePct     float64 // % move over window at signal
	FastMove    float64 // % move over 100ms window (flash)
	SecVolume   float64 // USDT notional in window
	FlashMode   string  // "full" | "early"
	BookMode    string  // "lead" | "violent"
	ImbalanceRatio float64
	BidNotional    float64
	AskNotional    float64
	ThinSideUSDT   float64
	TradeFlowUSDT  float64
	Mega        bool
	EntryPrice  float64 // aggTrade price at signal (dry tick entry)
	RecvAt      time.Time
}
