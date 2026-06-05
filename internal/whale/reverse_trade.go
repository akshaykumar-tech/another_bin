package whale

import (
	"strings"
)

// IsSymbolBlocked returns true if symbol is on burst.block_symbols denylist.
func (c Config) IsSymbolBlocked(symbol string) bool {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	for _, b := range c.Burst.BlockSymbols {
		if strings.ToUpper(strings.TrimSpace(b)) == sym {
			return true
		}
	}
	return false
}

// AllowsSignalSide returns whether burst detector side passes signal_sides filter (both/buy/sell).
func (c Config) AllowsSignalSide(side Side) bool {
	switch strings.ToLower(strings.TrimSpace(c.Burst.SignalSides)) {
	case "", "both", "all":
		return true
	case "buy", "long":
		return side == SideBuy
	case "sell", "short":
		return side == SideSell
	default:
		return true
	}
}

// TradeSide returns the side we actually trade (opposite of signal when reverse_trade).
func (c Config) TradeSide(signal Side) Side {
	if c.ReverseTrade {
		return oppositeSide(signal)
	}
	return signal
}

// RiskForExit returns risk params for exit logic (tighter SL on reverse when configured).
func (c Config) RiskForExit() Risk {
	r := c.Risk
	if c.ReverseTrade && c.ReverseStopLossPct > 0 {
		r.MegaStopLossPercent = c.ReverseStopLossPct
		r.StopLossPercent = c.ReverseStopLossPct
	}
	return r
}
