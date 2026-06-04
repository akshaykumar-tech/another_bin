package whale

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
