package binance

import "strings"

func extractParenTokens(title string) []string {
	start := strings.Index(title, "(")
	end := strings.Index(title, ")")
	if start < 0 || end <= start {
		return nil
	}
	raw := title[start+1 : end]
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.ToUpper(strings.TrimSpace(p))
		if s != "" && len(s) <= 12 {
			out = append(out, s)
		}
	}
	return out
}
