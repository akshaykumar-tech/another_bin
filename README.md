# crypto_announcements_go

Go rewrite scaffold for the Ruby `crypto_announcements` system with:

- same PostgreSQL database compatibility (reads/writes existing tables),
- same env-driven service split model (`all | binance | upbit`),
- Binance CMS websocket ingestion,
- Upbit API poll ingestion,
- low-latency trade-first orchestrator path.

## What is intentionally removed

- SMS/Twilio paths,
- browser scraping fallback,
- non-hot-path synchronous enrichment before trade.

## Run

1. Copy `.env.example` to `.env` and fill values.
2. Export env:
   - `set -a; source .env; set +a`
3. Start service:
   - `go run ./cmd/server`

## Notes

- Current implementation keeps DB schema parity and core control flow.
- Binance signed order endpoint is intentionally stubbed in `internal/binance/futures_client.go` for safe migration phase.
- Replace `MarketOrder()` with real signed `/fapi/v1/order` implementation before live use.
