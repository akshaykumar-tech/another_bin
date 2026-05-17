package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"crypto_announcements_go/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repo struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Repo { return &Repo{db: db} }

func (r *Repo) ExchangeByCode(ctx context.Context, code string) (*model.Exchange, error) {
	row := r.db.QueryRow(ctx, `SELECT id, code FROM exchanges WHERE code = $1 LIMIT 1`, code)
	var e model.Exchange
	if err := row.Scan(&e.ID, &e.Code); err != nil {
		return nil, err
	}
	return &e, nil
}

func (r *Repo) AnnouncementExists(ctx context.Context, exchangeID int64, title string) (bool, error) {
	var one int
	err := r.db.QueryRow(ctx, `SELECT 1 FROM announcements WHERE exchange_id=$1 AND title=$2 LIMIT 1`, exchangeID, title).Scan(&one)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (r *Repo) InsertAnnouncement(ctx context.Context, a model.Announcement) (int64, error) {
	var id int64
	err := r.db.QueryRow(ctx, `
		INSERT INTO announcements (
			exchange_id,title,content,announcement_type,severity,published_at,affected_tokens,recommended_action,raw_data,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),NOW()) RETURNING id
	`, a.ExchangeID, a.Title, a.Content, a.AnnouncementType, a.Severity, a.PublishedAt, a.AffectedTokens, a.RecommendedAction, a.RawData).Scan(&id)
	return id, err
}

func (r *Repo) TradingSetting(ctx context.Context) (model.TradingSetting, error) {
	// Columns match Rails db/schema.rb trading_settings (stop_loss_* from AddStopLossToTradingSettings migration).
	row := r.db.QueryRow(ctx, `
		SELECT enabled,dry_run,leverage,allocation_percent,max_tokens_to_trade,stop_loss_enabled,stop_loss_percent,announcement_actions
		FROM trading_settings ORDER BY id ASC LIMIT 1
	`)
	var s model.TradingSetting
	var actions []byte
	if err := row.Scan(&s.Enabled, &s.DryRun, &s.Leverage, &s.AllocationPercent, &s.MaxTokensToTrade, &s.StopLossEnabled, &s.StopLossPercent, &actions); err != nil {
		return s, err
	}
	_ = json.Unmarshal(actions, &s.AnnouncementActionsRaw)
	return s, nil
}

func (r *Repo) OpenTradeCount(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRow(ctx, `
		SELECT COUNT(*) FROM trade_executions
		WHERE status = ANY($1) AND (quantity_remaining IS NULL OR quantity_remaining > 0)
	`, []string{"pending", "submitted", "open", "first_exit_done"}).Scan(&n)
	return n, err
}

func (r *Repo) InsertTradeExecutionFailed(ctx context.Context, announcementID int64, symbol, base, side, errMsg string, raw map[string]any) error {
	body, _ := json.Marshal(raw)
	_, err := r.db.Exec(ctx, `
		INSERT INTO trade_executions (
			announcement_id,symbol,base_asset,position_side,status,last_error,manual_close_requested,raw_response,created_at,updated_at
		) VALUES ($1,$2,$3,$4,'failed',$5,false,$6,NOW(),NOW())
	`, announcementID, symbol, base, side, errMsg, body)
	return err
}

func (r *Repo) InsertTradeExecutionOpen(ctx context.Context, announcementID int64, symbol, base, side string, qty, entry float64, orderID string, raw map[string]any) error {
	body, _ := json.Marshal(raw)
	_, err := r.db.Exec(ctx, `
		INSERT INTO trade_executions (
			announcement_id,symbol,base_asset,position_side,status,quantity,quantity_remaining,entry_price,binance_client_order_id,manual_close_requested,raw_response,created_at,updated_at
		) VALUES ($1,$2,$3,$4,'open',$5,$5,$6,$7,false,$8,NOW(),NOW())
	`, announcementID, symbol, base, side, qty, entry, orderID, body)
	return err
}

func (r *Repo) BuildRawData(url string, synthetic bool, source string) []byte {
	m := map[string]any{
		"url":       url,
		"synthetic": synthetic,
		"source":    source,
		"ts":        time.Now().UTC().Format(time.RFC3339Nano),
	}
	b, _ := json.Marshal(m)
	return b
}

func (r *Repo) AnnouncementsBetween(ctx context.Context, start, end time.Time) ([]model.Announcement, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, exchange_id, title, content, announcement_type, severity, published_at, affected_tokens, recommended_action, raw_data
		FROM announcements
		WHERE published_at >= $1 AND published_at < $2
		ORDER BY published_at ASC
	`, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Announcement
	for rows.Next() {
		var a model.Announcement
		if err := rows.Scan(&a.ID, &a.ExchangeID, &a.Title, &a.Content, &a.AnnouncementType, &a.Severity,
			&a.PublishedAt, &a.AffectedTokens, &a.RecommendedAction, &a.RawData); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (r *Repo) AnnouncementsAfter(ctx context.Context, afterID int64, limit int) ([]model.Announcement, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.Query(ctx, `
		SELECT id, exchange_id, title, content, announcement_type, severity, published_at, affected_tokens, recommended_action, raw_data
		FROM announcements
		WHERE id > $1
		ORDER BY id ASC
		LIMIT $2
	`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Announcement
	for rows.Next() {
		var a model.Announcement
		if err := rows.Scan(&a.ID, &a.ExchangeID, &a.Title, &a.Content, &a.AnnouncementType, &a.Severity,
			&a.PublishedAt, &a.AffectedTokens, &a.RecommendedAction, &a.RawData); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (r *Repo) MustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("marshal: %w", err))
	}
	return b
}
