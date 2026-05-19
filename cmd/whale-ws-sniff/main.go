// Quick WS diagnostic: count aggTrade messages from Binance futures combined stream.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/config"
	"crypto_announcements_go/internal/whale"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

func main() {
	cfgPath := flag.String("config", "config/whale-loose-test.yaml", "")
	secs := flag.Int("sec", 30, "seconds to listen")
	nSyms := flag.Int("n", 3, "max symbols (0 = resolve full watchlist)")
	flag.Parse()

	_ = godotenv.Load()
	appCfg := config.Load()
	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	client := binance.NewFuturesClient("https://fapi.binance.com", appCfg.BinanceAPIKey, appCfg.BinanceAPISecret)
	if err := client.WarmSymbolCache(); err != nil {
		log.Fatal(err)
	}
	if err := cfg.ResolveWatchlist(client, client.USDTPerpetualSymbols()); err != nil {
		log.Fatal(err)
	}
	syms := cfg.Symbols
	if *nSyms > 0 && len(syms) > *nSyms {
		syms = syms[:*nSyms]
	}
	if len(syms) == 0 {
		syms = []string{"BTCUSDT"}
	}
	cfg.Symbols = syms
	url := buildURL(cfg)
	fmt.Printf("symbols=%d url_len=%d\n", len(syms), len(url))
	fmt.Printf("url_sample=%s...\n", url[:min(200, len(url))])

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*secs)*time.Second)
	defer cancel()

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("connected, counting messages...")

	var total, agg, other int
	kinds := make(map[string]int)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("done: total_msgs=%d aggTrades=%d other=%d in %ds\n", total, agg, other, *secs)
			fmt.Println("event kinds:")
			for k, v := range kinds {
				fmt.Printf("  %q: %d\n", k, v)
			}
			if agg == 0 {
				os.Exit(1)
			}
			return
		default:
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				fmt.Printf("done: total_msgs=%d aggTrades=%d other=%d (ctx done)\n", total, agg, other)
				fmt.Println("event kinds:")
				for k, v := range kinds {
					fmt.Printf("  %q: %d\n", k, v)
				}
				if agg == 0 {
					os.Exit(1)
				}
				return
			}
			log.Fatalf("read: %v", err)
		}
		total++
		var wrap struct {
			Data json.RawMessage `json:"data"`
		}
		payload := msg
		if json.Unmarshal(msg, &wrap) == nil && len(wrap.Data) > 0 {
			payload = wrap.Data
		}
		var peek struct {
			EventType string `json:"e"`
			EventTime int64  `json:"E"`
			Sym       string `json:"s"`
		}
		if json.Unmarshal(payload, &peek) != nil {
			other++
			kinds["<parse_fail>"]++
			continue
		}
		kinds[peek.EventType]++
		switch peek.EventType {
		case "aggTrade":
			agg++
			if agg <= 3 {
				fmt.Printf("sample aggTrade sym=%s\n", peek.Sym)
			}
		default:
			other++
			if other <= 3 {
				fmt.Printf("other event=%q sym=%s raw=%s\n", peek.EventType, peek.Sym, string(payload[:min(200, len(payload))]))
			}
		}
	}
}

func buildURL(cfg whale.Config) string {
	var parts []string
	for _, sym := range cfg.Symbols {
		parts = append(parts, strings.ToLower(sym)+"@aggTrade")
	}
	root := strings.TrimRight(cfg.WebSocket.URL, "/")
	for _, suf := range []string{"/ws", "/public", "/market", "/private"} {
		if strings.HasSuffix(root, suf) {
			root = strings.TrimSuffix(root, suf)
			break
		}
	}
	return root + "/market/stream?streams=" + strings.Join(parts, "/")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
