package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"crypto_announcements_go/internal/model"
	"crypto_announcements_go/internal/repo"
	"crypto_announcements_go/internal/trading"

	"github.com/cenkalti/backoff/v4"
	"github.com/gorilla/websocket"
)

type AnnouncementStream struct {
	baseURL string
	topic   string
	apiKey  string
	secret  string
	repo    *repo.Repo
	trader  *trading.Orchestrator
}

func NewAnnouncementStream(baseURL, topic, apiKey, secret string, r *repo.Repo, t *trading.Orchestrator) *AnnouncementStream {
	return &AnnouncementStream{
		baseURL: strings.TrimRight(baseURL, "/"),
		topic:   topic,
		apiKey:  apiKey,
		secret:  secret,
		repo:    r,
		trader:  t,
	}
}

func (s *AnnouncementStream) Run(ctx context.Context) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 500 * time.Millisecond
	bo.MaxElapsedTime = 0

	return backoff.Retry(func() error {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		err := s.runOnce(ctx)
		if err != nil {
			log.Printf("[binance_ws] reconnect after error: %v", err)
		}
		return err
	}, backoff.WithContext(bo, ctx))
}

func (s *AnnouncementStream) runOnce(ctx context.Context) error {
	if strings.TrimSpace(s.apiKey) == "" || strings.TrimSpace(s.secret) == "" {
		return errors.New("missing BINANCE_API_KEY or BINANCE_API_SECRET")
	}

	wsURL := s.signedURL()
	h := map[string][]string{"X-MBX-APIKEY": {s.apiKey}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, h)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"command": "SUBSCRIBE", "value": s.topic}); err != nil {
		return fmt.Errorf("failed to send subscribe command: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pingTicker.C:
				_ = conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(2*time.Second))
			}
		}
	}()

	ex, err := s.repo.ExchangeByCode(ctx, "binance")
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				return fmt.Errorf("server closed websocket normally; verify API key/secret and topic: %w", err)
			}
			return err
		}
		var env struct {
			Type  string          `json:"type"`
			Topic string          `json:"topic"`
			Data  json.RawMessage `json:"data"`
			Code  string          `json:"code"`
		}
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Printf("[binance_ws] non-json message: %s", string(msg))
			continue
		}
		if env.Type == "COMMAND" {
			log.Printf("[binance_ws] command response: code=%s data=%s", env.Code, string(env.Data))
			continue
		}
		if env.Type != "DATA" || env.Topic != s.topic {
			log.Printf("[binance_ws] ignored event type=%s topic=%s payload=%s", env.Type, env.Topic, string(msg))
			continue
		}

		var article struct {
			Title       string `json:"title"`
			Body        string `json:"body"`
			Code        string `json:"code"`
			ReleaseDate any    `json:"releaseDate"`
		}
		rawData := env.Data
		if len(rawData) > 0 && rawData[0] == '"' {
			var unquoted string
			if json.Unmarshal(rawData, &unquoted) == nil {
				rawData = []byte(unquoted)
			}
		}
		if err := json.Unmarshal(rawData, &article); err != nil {
			log.Printf("[binance_ws] announcement recv_ts=%s classified=— title=(parse_error) err=%v data=%s",
				time.Now().UTC().Format(time.RFC3339Nano), err, truncateRunes(string(rawData), 500))
			continue
		}
		title := strings.TrimSpace(article.Title)
		typ, sev, action := classifyBinance(title)
		clsLabel := typ
		if clsLabel == "" {
			clsLabel = "unclassified"
		}
		if title == "" {
			log.Printf("[binance_ws] announcement recv_ts=%s classified=%s title=(empty) code=%q releaseDate=%v",
				time.Now().UTC().Format(time.RFC3339Nano), clsLabel, article.Code, article.ReleaseDate)
			continue
		}
		log.Printf("[binance_ws] announcement recv_ts=%s classified=%s title=%q code=%q releaseDate=%v",
			time.Now().UTC().Format(time.RFC3339Nano), clsLabel, title, article.Code, article.ReleaseDate)

		if typ == "" {
			continue
		}

		exists, _ := s.repo.AnnouncementExists(ctx, ex.ID, title)
		if exists {
			continue
		}

		affected := extractParenTokens(title)
		if len(affected) == 0 {
			continue
		}

		// FAST PATH: Insert announcement and fire trade concurrently.
		// DB insert gets the ID needed for trade_executions FK; we use a channel to pass it.
		idCh := make(chan int64, 1)
		go func() {
			urlStr := "https://www.binance.com/en/support/announcement"
			if strings.TrimSpace(article.Code) != "" {
				urlStr = "https://www.binance.com/en/support/announcement/detail/" + strings.TrimSpace(article.Code)
			}
			id, err := s.repo.InsertAnnouncement(ctx, model.Announcement{
				ExchangeID:        ex.ID,
				Title:             title,
				Content:           strings.TrimSpace(article.Body),
				AnnouncementType:  typ,
				Severity:          sev,
				PublishedAt:       time.Now().UTC(),
				AffectedTokens:    affected,
				RecommendedAction: action,
				RawData:           s.repo.BuildRawData(urlStr, false, "binance_ws"),
			})
			if err != nil {
				log.Printf("[binance_ws] insert announcement failed: %v", err)
				idCh <- 0
				return
			}
			idCh <- id
		}()

		// Wait for announcement ID (needed for FK in trade_executions), then trade.
		annID := <-idCh
		if annID == 0 {
			continue
		}
		_ = s.trader.HandleAnnouncement(ctx, model.Announcement{
			ID:               annID,
			ExchangeID:       ex.ID,
			Title:            title,
			Content:          strings.TrimSpace(article.Body),
			AnnouncementType: typ,
			AffectedTokens:   affected,
		})
	}
}

func (s *AnnouncementStream) signedURL() string {
	q := url.Values{}
	q.Set("random", fmt.Sprintf("%d", time.Now().UnixNano()))
	q.Set("topic", s.topic)
	q.Set("recvWindow", "30000")
	q.Set("timestamp", fmt.Sprintf("%d", time.Now().UnixMilli()))
	query := q.Encode()
	mac := hmac.New(sha256.New, []byte(s.secret))
	_, _ = mac.Write([]byte(query))
	sig := hex.EncodeToString(mac.Sum(nil))
	return s.baseURL + "?" + query + "&signature=" + sig
}

func truncateRunes(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "…"
}

func classifyBinance(title string) (typ, severity, action string) {
	t := strings.ToLower(title)

	// "Binance Alpha" announcements are a separate discovery platform — not real exchange listings/delistings.
	if strings.Contains(t, "binance alpha") {
		return "alpha_update", "low", "none"
	}

	switch {
	case containsAny(t, "will list", "new listing", "will launch", "opens trading for", "futures will launch", "perpetual contract", "launchpool", "megadrop", "hodler airdrops"):
		return "listing", "critical", "buy"
	case containsAny(t, "will delist", "delisting", "notice of removal", "will remove", "cease trading", "trading will be terminated", "suspend trading"):
		return "delisting", "critical", "sell"
	case containsAny(t, "adds seed tag", "monitoring tag removed", "expanded earn", "new margin pairs", "new borrowable asset"):
		return "ecosystem_positive", "high", "buy"
	case containsAny(t, "monitoring tag", "seed tag", "high risk", "bankruptcy", "investigation", "regulatory"):
		return "risk_warning", "high", "sell"
	case containsAny(t, "network upgrade", "wallet maintenance", "suspension of deposits", "tick size", "api update", "system maintenance"):
		return "maintenance", "low", "none"
	default:
		return "", "", ""
	}
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}
