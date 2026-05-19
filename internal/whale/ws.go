package whale

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type depthEvent struct {
	EventType string     `json:"e"`
	EventTime int64      `json:"E"` // must be present: json "e"/"E" match is case-insensitive in Go
	Symbol    string     `json:"s"`
	Bids      [][]string `json:"b"`
	Asks      [][]string `json:"a"`
}

type aggTradeEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	Maker     bool   `json:"m"`
	TimeMs    int64  `json:"T"` // exchange trade time (ms)
}

type combinedWrapper struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// StreamEvent is queued from the WS read loop and processed by workers.
type StreamEvent struct {
	Symbol string
	Recv   time.Time
	Bids   [][]string
	Asks   [][]string
	Trade  *aggTradeEvent
}

type FuturesWS struct {
	cfg    Config
	events chan StreamEvent

	dropped  atomic.Uint64
	enqueued atomic.Uint64
	received atomic.Uint64
}

func NewFuturesWS(cfg Config) *FuturesWS {
	n := len(cfg.Symbols)
	buf := n * 8
	if n >= 200 && buf < 12288 {
		buf = 12288 // larger watchlists: reduce aggTrade drops under bursts
	}
	if buf < 4096 {
		buf = 4096
	}
	if buf > 65536 {
		buf = 65536
	}
	return &FuturesWS{
		cfg:    cfg,
		events: make(chan StreamEvent, buf),
	}
}

func (w *FuturesWS) Events() <-chan StreamEvent {
	return w.events
}

func chunkSymbols(symbols []string, size int) [][]string {
	if size <= 0 {
		size = 80
	}
	var chunks [][]string
	for i := 0; i < len(symbols); i += size {
		end := i + size
		if end > len(symbols) {
			end = len(symbols)
		}
		chunks = append(chunks, symbols[i:end])
	}
	return chunks
}

// futuresRootURL strips legacy /ws and routed /public|/market|/private suffixes.
func futuresRootURL(raw string) string {
	base := strings.TrimRight(raw, "/")
	for _, suf := range []string{"/ws", "/public", "/market", "/private"} {
		if strings.HasSuffix(base, suf) {
			return strings.TrimSuffix(base, suf)
		}
	}
	return base
}

// buildRoutedStreamURL builds a combined-stream URL on Binance's routed endpoints.
// aggTrade → /market; partial depth → /public (see Binance WS migration notice).
func buildRoutedStreamURL(root, route string, streamParts []string) string {
	if len(streamParts) == 0 {
		return ""
	}
	streamPath := strings.Join(streamParts, "/")
	return root + "/" + route + "/stream?streams=" + streamPath
}

func (w *FuturesWS) depthSuffix() string {
	levels := w.cfg.DepthLevels
	if w.cfg.UsesBookLead() && w.cfg.BookLead.DepthLevels > 0 {
		levels = w.cfg.BookLead.DepthLevels
	}
	if levels <= 0 {
		levels = 10
	}
	interval := w.cfg.UpdateIntervalMs
	if interval <= 0 {
		interval = 100
	}
	return fmt.Sprintf("@depth%d@%dms", levels, interval)
}

func (w *FuturesWS) streamURLs(symbols []string) (publicURL, marketURL string) {
	root := futuresRootURL(w.cfg.WebSocket.URL)
	depthSuf := w.depthSuffix()
	var publicParts, marketParts []string
	for _, sym := range symbols {
		s := strings.ToLower(sym)
		if w.cfg.UsesBookLead() {
			publicParts = append(publicParts, s+depthSuf)
			marketParts = append(marketParts, s+"@aggTrade")
		} else {
			marketParts = append(marketParts, s+"@aggTrade")
		}
	}
	return buildRoutedStreamURL(root, "public", publicParts),
		buildRoutedStreamURL(root, "market", marketParts)
}

func (w *FuturesWS) Run(ctx context.Context) error {
	chunks := chunkSymbols(w.cfg.Symbols, w.cfg.SymbolsPerConnection)
	if len(chunks) == 0 {
		return fmt.Errorf("no symbols to subscribe")
	}
	mode := "aggTrade only"
	if w.cfg.UsesBookLead() {
		mode = "depth+aggTrade"
	} else if w.cfg.UsesBurst() {
		mode = "burst aggTrade"
	}
	log.Printf("[whale_ws] subscribing %d symbols across %d connections (%s)",
		len(w.cfg.Symbols), len(chunks), mode)

	var wg sync.WaitGroup
	for i, chunk := range chunks {
		publicURL, marketURL := w.streamURLs(chunk)
		if publicURL != "" {
			wg.Add(1)
			go func(id int, url string, n int) {
				defer wg.Done()
				w.reconnectEndpoint(ctx, id, url, n, "public")
			}(i*2, publicURL, len(chunk))
		}
		if marketURL != "" {
			wg.Add(1)
			id := i*2 + 1
			if publicURL == "" {
				id = i
			}
			go func(id int, url string, n int) {
				defer wg.Done()
				w.reconnectEndpoint(ctx, id, url, n, "market")
			}(id, marketURL, len(chunk))
		}
	}

	wg.Wait()
	return ctx.Err()
}

// ConsumeStats returns WS counters since last call and resets them.
func (w *FuturesWS) ConsumeStats() (recv, enqueued, dropped uint64) {
	return w.received.Swap(0), w.enqueued.Swap(0), w.dropped.Swap(0)
}

func (w *FuturesWS) reconnectEndpoint(ctx context.Context, id int, url string, symCount int, route string) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := w.runEndpoint(ctx, id, url, symCount, route)
		if ctx.Err() != nil {
			return
		}
		log.Printf("[whale_ws] conn#%d (%s) disconnected (%d symbols): %v — retry in %s",
			id, route, symCount, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (w *FuturesWS) runEndpoint(ctx context.Context, id int, url string, symCount int, route string) error {
	log.Printf("[whale_ws] conn#%d (%s) connecting %d symbols url_len=%d", id, route, symCount, len(url))

	dialer := websocket.Dialer{
		HandshakeTimeout:  15 * time.Second,
		ReadBufferSize:    1 << 20,
		WriteBufferSize:   1 << 20,
		EnableCompression: false,
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("[whale_ws] conn#%d (%s) connected", id, route)

	const readWait = 10 * time.Minute
	refreshDeadline := func() {
		_ = conn.SetReadDeadline(time.Now().Add(readWait))
	}
	refreshDeadline()

	conn.SetPingHandler(func(appData string) error {
		refreshDeadline()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})
	conn.SetPongHandler(func(string) error {
		refreshDeadline()
		return nil
	})

	go w.pingLoop(ctx, conn, refreshDeadline)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			// Do not ReadMessage again on this conn — gorilla marks it failed (incl. read deadline).
			return err
		}
		refreshDeadline()
		w.enqueue(msg, time.Now())
	}
}

func (w *FuturesWS) enqueue(msg []byte, recv time.Time) {
	w.received.Add(1)
	var wrap combinedWrapper
	payload := msg
	if err := json.Unmarshal(msg, &wrap); err == nil && len(wrap.Data) > 0 {
		payload = wrap.Data
	}
	var peek struct {
		EventType string `json:"e"`
		EventTime int64  `json:"E"`
	}
	if err := json.Unmarshal(payload, &peek); err != nil {
		return
	}
	var ev StreamEvent
	ev.Recv = recv
	switch peek.EventType {
	case "depthUpdate":
		var d depthEvent
		if json.Unmarshal(payload, &d) != nil {
			return
		}
		ev.Symbol = d.Symbol
		ev.Bids = d.Bids
		ev.Asks = d.Asks
	case "aggTrade":
		var t aggTradeEvent
		if json.Unmarshal(payload, &t) != nil {
			return
		}
		ev.Symbol = t.Symbol
		ev.Trade = &t
	default:
		return
	}
	select {
	case w.events <- ev:
		w.enqueued.Add(1)
	default:
		n := w.dropped.Add(1)
		if n == 1 || n%5000 == 0 {
			log.Printf("[whale_ws] WARNING: dropped %d aggTrade/depth events (queue full)", n)
		}
	}
}

func (w *FuturesWS) pingLoop(ctx context.Context, conn *websocket.Conn, refresh func()) {
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			refresh()
			_ = conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second))
		}
	}
}

func (w *FuturesWS) Reconnect(ctx context.Context) {
	_ = w.Run(ctx)
}

// tradeEventTime returns Binance trade time when present; otherwise receive time.
func tradeEventTime(recv time.Time, t *aggTradeEvent) time.Time {
	if t != nil && t.TimeMs > 0 {
		return time.UnixMilli(t.TimeMs).UTC()
	}
	return recv
}
