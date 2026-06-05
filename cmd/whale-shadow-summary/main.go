// Summarize SHADOW_EXIT lines from whale-trades.log (probe-order PnL).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	path := flag.String("log", "whale-trades.log", "trade journal path")
	flag.Parse()

	f, err := os.Open(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", *path, err)
		os.Exit(1)
	}
	defer f.Close()

	var n int
	var sumPnL float64
	var wins, losses int
	reasons := make(map[string]int)

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "SHADOW_EXIT") {
			continue
		}
		n++
		if r := field(line, "reason="); r != "" {
			reasons[r]++
		}
		if v, ok := parseField(line, "pnl_usdt="); ok {
			sumPnL += v
			if v > 0 {
				wins++
			} else if v < 0 {
				losses++
			}
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("=== shadow summary: %s ===\n", *path)
	fmt.Printf("trades: %d | wins: %d | losses: %d | flat: %d\n", n, wins, losses, n-wins-losses)
	fmt.Printf("total pnl_usdt: %+.2f\n", sumPnL)
	if n > 0 {
		fmt.Printf("avg pnl_usdt/trade: %+.2f\n", sumPnL/float64(n))
	}
	if len(reasons) > 0 {
		fmt.Println("exit reasons:")
		for k, c := range reasons {
			fmt.Printf("  %s: %d\n", k, c)
		}
	}
}

func field(line, key string) string {
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key):]
	if j := strings.IndexByte(rest, '\t'); j >= 0 {
		return rest[:j]
	}
	return strings.Fields(rest)[0]
}

func parseField(line, key string) (float64, bool) {
	s := field(line, key)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}
