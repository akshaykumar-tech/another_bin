# Whale bot fixes (after `2ea49c2`)

Testing commits (`0e5e0b3` … `5a79b68`) were squashed into one changeset. Summary for EC2/local deploy.

## 1. Exit deadlock (main “stuck after entry” bug)

**Symptom:** `focus ON` → `PROBE OPEN` → no `EXIT` for minutes (only `timeout` at 10m).

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

## 5. Shadow probe mode (no real positions)

`WHALE_SHADOW_LIVE=true`: real MARKET order sized to fail (`margin × leverage` notional), record mark price at reject, log `SHADOW_ENTRY` / `SHADOW_EXIT` with virtual PnL.

`WHALE_REVERSE_LIVE=false` when shadow on.

## 6. Reverse trade

`WHALE_REVERSE_TRADE=true`: signal BUY → trade SELL. `WHALE_REVERSE_SL_PERCENT` for exit SL.

## 7. Race fixes (ICNT-style)

- Sync shadow open under focus; skip live if sim already closed.
- `closing` map avoids duplicate EXIT logs.
- `countOpenSlotsLocked()` includes dry + shadow positions.

## 8. Tooling

- `cmd/whale-shadow-summary` — summarize `SHADOW_EXIT` lines in `whale-trades.log`.
- `.gitignore`: `bin/`, `whale-trades.log`.

## Deploy

```bash
go build -o bin/whale ./cmd/whale/
./bin/whale   # not ./whale (stale binary in repo root)
```

Config: `config/whale.yaml` — `watchlist.size: 300`, `focus_cooldown_sec: 120`.
