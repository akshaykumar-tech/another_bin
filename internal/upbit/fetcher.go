package upbit

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"crypto_announcements_go/internal/model"
	"crypto_announcements_go/internal/repo"
	"crypto_announcements_go/internal/trading"

	"github.com/go-resty/resty/v2"
)

type Fetcher struct {
	apiURL     string
	perPage    int
	onlyLatest bool
	logSkipped bool
	http       *resty.Client
	repo       *repo.Repo
	trader     *trading.Orchestrator
}

func New(apiURL string, perPage int, onlyLatest bool, logSkipped bool, r *repo.Repo, t *trading.Orchestrator) *Fetcher {
	return &Fetcher{
		apiURL:     apiURL,
		perPage:    perPage,
		onlyLatest: onlyLatest,
		logSkipped: logSkipped,
		http:       resty.New().SetTimeout(8 * time.Second).SetRetryCount(1),
		repo:       r,
		trader:     t,
	}
}

func (f *Fetcher) Poll(ctx context.Context) error {
	ex, err := f.repo.ExchangeByCode(ctx, "upbit")
	if err != nil {
		return err
	}
	var payload struct {
		Data struct {
			Notices []struct {
				Title    string `json:"title"`
				ListedAt string `json:"listed_at"`
				ID       int64  `json:"id"`
			} `json:"notices"`
		} `json:"data"`
	}
	resp, err := f.http.R().
		SetResult(&payload).
		SetQueryParam("os", "web").
		SetQueryParam("category", "trade").
		SetQueryParam("page", "1").
		SetQueryParam("per_page", fmt.Sprintf("%d", f.perPage)).
		Get(f.apiURL)
	if err != nil {
		return err
	}
	if resp.StatusCode() >= 300 {
		return fmt.Errorf("upbit status=%d", resp.StatusCode())
	}
	rows := payload.Data.Notices
	if f.onlyLatest && len(rows) > 1 {
		rows = rows[:1]
	}
	for _, n := range rows {
		title := strings.TrimSpace(n.Title)
		if title == "" {
			continue
		}
		exists, _ := f.repo.AnnouncementExists(ctx, ex.ID, title)
		if exists {
			continue
		}
		typ, sev, action := classify(title)
		if typ == "" {
			if f.logSkipped {
				log.Printf("[upbit] skipped unclassified announcement id=%d title=%q", n.ID, title)
			}
			continue
		}
		tokens := extractParenTokens(title)
		pub, _ := time.Parse(time.RFC3339, n.ListedAt)
		raw := f.repo.BuildRawData(fmt.Sprintf("https://www.upbit.com/service_center/notice?id=%d", n.ID), false, "upbit_api")
		a := model.Announcement{
			ExchangeID:        ex.ID,
			Title:             title,
			Content:           title,
			AnnouncementType:  typ,
			Severity:          sev,
			PublishedAt:       pub,
			AffectedTokens:    tokens,
			RecommendedAction: action,
			RawData:           raw,
		}
		id, err := f.repo.InsertAnnouncement(ctx, a)
		if err != nil {
			return err
		}
		if f.trader != nil {
			a.ID = id
			if err := f.trader.HandleAnnouncement(ctx, a); err != nil {
				// Keep polling resilient: announcement is already persisted.
				// Errors are recorded by orchestrator per-symbol when applicable.
			}
		}
	}
	return nil
}

func classify(title string) (typ, severity, action string) {
	t := strings.ToLower(title)
	switch {
	case strings.Contains(t, "market support"),
		strings.Contains(title, "거래지원"),
		strings.Contains(title, "디지털 자산 추가"),
		strings.Contains(title, "마켓 디지털 자산 추가"):
		return "market_support", "critical", "buy"
	case strings.Contains(t, "delist"),
		strings.Contains(t, "termination"),
		strings.Contains(title, "거래지원 종료"):
		return "delisting", "critical", "sell"
	default:
		return "", "", ""
	}
}

func extractParenTokens(title string) []string {
	start := strings.Index(title, "(")
	end := strings.Index(title, ")")
	if start >= 0 && end > start {
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
	return nil
}
