package binance

import (
	"regexp"
	"strings"
)

// extractParenTokens extracts token symbols from announcement titles.
// Handles two formats:
//   - Parenthesized: "... adds (BTC, ETH) ..." → [BTC, ETH]
//   - Inline after keyword: "Will Delist ATA, FARM, MLN on 2026-05-27" → [ATA, FARM, MLN]
func extractParenTokens(title string) []string {
	// Try parenthesized first.
	start := strings.Index(title, "(")
	end := strings.Index(title, ")")
	if start >= 0 && end > start {
		if tokens := splitTokenList(title[start+1 : end]); len(tokens) > 0 {
			return tokens
		}
	}

	// Fallback: extract comma-separated uppercase symbols from the title body.
	// Match sequences like "ATA, FARM, MLN, PHB, SYS" (2-12 uppercase letters separated by ", ").
	re := regexp.MustCompile(`\b([A-Z][A-Z0-9]{1,11})(?:\s*,\s*([A-Z][A-Z0-9]{1,11}))+\b`)
	m := re.FindString(title)
	if m != "" {
		return splitTokenList(m)
	}

	// USDT pair fallback: "PHAROSUSDT and STARUSDT" or single "ONDOUSDT"
	reUSDT := regexp.MustCompile(`\b([A-Z][A-Z0-9]{1,11}?)USDT\b`)
	matches := reUSDT.FindAllStringSubmatch(title, -1)
	if len(matches) > 0 {
		var tokens []string
		for _, sm := range matches {
			if isTokenLike(sm[1]) {
				tokens = append(tokens, sm[1])
			}
		}
		if len(tokens) > 0 {
			return tokens
		}
	}
	return nil
}

func splitTokenList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.ToUpper(strings.TrimSpace(p))
		if s == "" || len(s) > 12 {
			continue
		}
		if !isTokenLike(s) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func isTokenLike(s string) bool {
	if len(s) < 2 {
		return false
	}
	for _, c := range s {
		if !((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}
