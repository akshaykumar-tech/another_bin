# Whale bot fixes (after `2ea49c2`)

Testing commits were squashed into one changeset. Summary for EC2/local deploy.

## 1. Exit deadlock (main “stuck after entry” bug)

**Symptom:** `focus ON` → entry → no `EXIT` for minutes (only `timeout` at 10m).

**Cause:** `OnPriceTick` / mark poll held `e.mu.Lock()` and called `dryExitStep()`, which tried `e.mu.Lock()` again on exit → deadlock.

**Fix:** Unlock before `dryExitStep()`; re-lock only to delete `dryOpen`.

## 2. Exit without aggTrade ticks

**Symptom:** Thin coins: few trades after entry → `no_mega` never evaluated.

**Fix:** Always run `manageDryRunMarkPoll` (500ms mark price) alongside tick mode, not only `dryRunTimeout` (10m).

## 3. Focus mode + 2m cooldown

On signal: pause all other symbols (WS enqueue filter + worker skip). After trade close: `focus_cooldown_sec` (default 120s) pause, then resume 300-symbol scan.

Files: `internal/whale/focus.go`, `runner.go`, `ws.go`, `executor.go`.

## 4. WS queue drops (300 symbols)

**Symptom:** `dropped N aggTrade events (queue full)`.

**Fix:** Enqueue only focused symbol during trade; larger queue for 200+ symbols; more workers; skip non-open symbols for `OnPriceTick`.

## 5. Reverse trade

`WHALE_REVERSE_TRADE=true`: signal BUY → trade SELL. `WHALE_REVERSE_SL_PERCENT` for exit SL.

## 6. Race fixes

- `closing` map + `claimDryExit` — single EXIT path (tick + mark poll).
- SL grace: `mega_trail_min_hold_ms` applies to stop-loss (no 0s tick SL).
- `countOpenSlotsLocked()` includes dry + live reverse positions.
- `dryOpenSyms` for fast `HasDryPosition`.

## 7. Entry / exit tuning (loss on thin 0.5–1% bursts)

- `early_capture_all: false` — restore pre-trade + momentum gates.
- `min_sec_move_pct: 1.2`, `min_fast_sec_ratio: 0.50` — block late 1s chase (TAKE/ELSA).
- `min_sec_notional_usdt: 15000`, `max_entry_sec_move_pct: 0.65`.
- Smaller-move exits: `mega_confirm_min_favorable_pct: 0.25`, window 45s, trail activate 0.35%, SL 1%.
- Live leverage: `WHALE_LEVERAGE` forces live size (capped by `max_leverage_cap: 10`).

## Deploy

```bash
go build -o bin/whale ./cmd/whale/
./bin/whale   # not ./whale (stale binary in repo root)
```

Config: `config/whale.yaml` — `watchlist.size: 300`, `focus_cooldown_sec: 120`.
