package whale

// applySlippage worsens fill price for entry/exit simulation (bps). Entry on open, exit on close.
func applySlippage(price float64, side Side, bps float64, isEntry bool) float64 {
	if price <= 0 || bps <= 0 {
		return price
	}
	m := bps / 10000
	if isEntry {
		if side == SideBuy {
			return price * (1 + m)
		}
		return price * (1 - m)
	}
	if side == SideBuy {
		return price * (1 - m)
	}
	return price * (1 + m)
}
