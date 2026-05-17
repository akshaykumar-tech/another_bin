# Burst Bot Backtest Comparison

**Strategy:** `pump_only` burst + mega trailing exit  
**Capital:** 1,000 USDT | **Mega risk:** 5% margin (~$50/trade)  
**Windows:** 13 & 15 May 2026 → **11:00–17:00 IST** (AIGENSYN 14 May → **11:00–19:00 IST**)

---

## What changed between runs

| | Pehle (bina pre-trade) | Ab (pre-trade ON) |
|--|------------------------|-------------------|
| Pump / cascade filters | ✅ | ✅ |
| Cascade + exit fixes | ✅ | ✅ |
| **Pre-trade gate** | ❌ | ✅ `max_quiet_60s`, `max_range_60s`, `max_prior_1s_move_60s` |
| Pre-trade window | — | 60s **before** entry (last 1s burst leg excluded) |

**Pre-trade config (`config/whale.yaml`):**
```yaml
pre_trade_enabled: true
pre_trade_window_ms: 60000
max_quiet_60s_usdt: 40000
max_range_60s_pct: 0.75
max_prior_1s_move_60s_pct: 0.35
```

**Idea:** Sirf tab trade jab entry se pehle tape **dead + flat** ho (13 May mega / 14 May AIGEN profile).

---

## 13 May 2026 (11:00–17:00 IST)

### Per symbol

| Token | Pehle: Trades | Pehle: PnL | Ab: Trades | Ab: PnL | Mega @ 13:30 |
|-------|---------------|------------|------------|---------|--------------|
| **SYS** | 3 | +2.16 | **2** | **+4.61** | ✅ → ✅ |
| **MLN** | 5 | +4.05 | **1** | **+4.28** | ✅ → ✅ |
| **ATA** | 5 | +4.80 | **1** | +0.04 | ❌ → ❌ |
| **PHB** | 7 | +2.06 | **6** | −0.26 | ❌ → ❌ |
| **TOTAL (4)** | **20** | **+13.07** | **10** | **+8.67** | |

### Pehle — trade list

**SYS (+2.16)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 13:30:06 | SELL | mega_tp | +3.00 |
| 14:10:42 | BUY | sl | −1.04 |
| 14:43:59 | SELL | sl | −1.06 |

**MLN (+4.05)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 13:30:06 | SELL | mega_tp | +3.03 |
| 14:00:13 | BUY | trail | +0.83 |
| 14:36:15 | SELL | sl | −1.00 |
| 15:21:56 | BUY | sl | −1.01 |
| 16:12:23 | SELL | timeout | −0.29 |

**ATA (+4.80)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 13:07:00 | BUY | timeout | +0.04 |
| 13:38:05 | BUY | trail | +0.70 |
| 14:30:30 | SELL | sl | −1.07 |
| 15:10:15 | SELL | trail | +1.40 |
| 16:26:32 | BUY | trail | +1.21 |

**PHB (+2.06)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 12:34:36 | BUY | timeout | −0.34 |
| 13:29:00 | SELL | trail | +0.69 |
| 14:00:23 | BUY | trail | +1.41 |
| 14:31:08 | SELL | timeout | +0.68 |
| 15:03:43 | SELL | sl | −1.00 |
| 15:54:25 | SELL | timeout | −0.68 |
| 16:26:11 | BUY | timeout | +0.05 |

### Ab (pre-trade) — trade list

**SYS (+4.61)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 13:30:06 | SELL | mega_tp | +3.00 |
| 15:05:46 | SELL | timeout | +0.36 |

**MLN (+4.28)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 13:30:06 | SELL | mega_tp | +3.03 |

**ATA (+0.04)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 13:07:00 | BUY | timeout | +0.04 |

**PHB (−0.26)**  
| Entry IST | Side | Exit | PnL USDT |
|-----------|------|------|----------|
| 12:36:02 | BUY | timeout | −0.61 |
| 13:29:00 | SELL | trail | +0.69 |
| 14:31:08 | SELL | timeout | +0.68 |
| 15:03:43 | SELL | sl | −1.00 |
| 16:07:02 | BUY | timeout | −0.49 |
| 16:38:50 | SELL | timeout | −0.78 |

### 13 May — summary

| Metric | Pehle | Ab |
|--------|-------|-----|
| Total trades | 20 | **10 (−50%)** |
| Gross PnL | +13.07 | +8.67 |
| Mega catches (SYS+MLN) | 2 | **2 kept** |
| Chop SL/trail noise | 18 trades | **8 trades** |

> **Note:** 13 May coordinated dump @ 13:30 — SYS & MLN mega safe. ATA/PHB dump miss dono runs mein. Ab ATA/PHB ke false positives kam, lekin PHB par abhi bhi 6 chop trades.

---

## 15 May 2026 (11:00–17:00 IST)

### Per symbol

| Token | Pehle: Trades | Pehle: PnL | Ab: Trades | Ab: PnL |
|-------|---------------|------------|------------|---------|
| **SYS** | 3 | −0.78 | **1** | **−0.09** |
| **MLN** | 8 | +0.24 | **4** | +0.35 |
| **ATA** | 0 | 0.00 | **0** | 0.00 |
| **PHB** | 1 | +0.51 | **0** | 0.00 |
| **AIGENSYN** | 0 | 0.00 | **0** | 0.00 |
| **TOTAL** | **12** | **−0.03** | **5** | **+0.26** |

### Pehle — trade list

**SYS (−0.78)** — 12:16 SELL −0.08 | 13:47 BUY −0.61 | 14:20 SELL −0.09  

**MLN (+0.24)** — 8 trades (timeouts + 1 SL), net +0.24  

**PHB (+0.51)** — 14:39:30 SELL timeout +0.51  

**ATA / AIGENSYN** — 0 trades  

### Ab (pre-trade) — trade list

**SYS (−0.09)** — 14:20:03 SELL timeout −0.09  

**MLN (+0.35)** — 13:10 SELL −0.30 | 14:16 BUY −0.02 | 15:32 BUY +0.92 | 16:03 SELL −0.25  

**ATA / PHB / AIGENSYN** — 0 trades  

### 15 May — summary

| Metric | Pehle | Ab |
|--------|-------|-----|
| Total trades | 12 | **5 (−58%)** |
| Gross PnL | −0.03 | **+0.26** |
| Mega | 0 | 0 |

> Quiet/chop din — pre-trade ne trades kam kiye, **loss → small profit** (fees ke baad ~flat).

---

## AIGENSYN 14 May (reference — 11:00–19:00 IST)

| | Pehle | Ab (pre-trade) |
|--|-------|----------------|
| Trades | 3 | **1** |
| PnL | +3.17 | **+4.25** |
| 15:30 mega | ✅ +12% | ✅ +12% (11.94% capture) |

**Ab:** Sirf 15:30:05 BUY → mega_tp; 14:30 & 16:55 chop trades filtered.

---

## Fees estimate (taker 0.04%, ~$50 notional/trade)

| Day | Pehle trades | Ab trades | Pehle fees ~ | Ab fees ~ | Pehle net ~ | Ab net ~ |
|-----|--------------|-----------|--------------|-----------|-------------|----------|
| 13 May (4 sym) | 20 | 10 | −0.83 | −0.42 | **+12.24** | **+8.25** |
| 15 May (5 sym) | 12 | 5 | −0.48 | −0.20 | **−0.51** | **+0.06** |

---

## Key takeaways

1. **Pre-trade mega ko safe rakhta hai** — SYS/MLN 13 May & AIGEN 14 May mega_tp same.
2. **Trade count ~50% kam** — fees aur chop exposure dono kam.
3. **13 May:** Gross thoda kam (+8.67 vs +13.07) kyunki ATA/PHB ke profitable chop trades bhi gaye; SYS/MLN **better** (+4.61, +4.28 vs +2.16, +4.05).
4. **15 May:** −0.03 → **+0.26** gross — normal din par filter useful.
5. **Abhi bhi open:** ATA/PHB 13:30 dump miss; PHB 13:29 false trail; MLN 15 May par 4 chhoti trades — next: market-sync + follow-through confirm.

---

## Commands to reproduce

```bash
# 13 May example
go run ./cmd/whale-replay/ -symbol MLNUSDT \
  -start 2026-05-13T11:00:00+05:30 \
  -end 2026-05-13T17:00:00+05:30 -burst -verbose

# 14 May AIGEN (wider window)
go run ./cmd/whale-replay/ -symbol AIGENSYNUSDT \
  -start 2026-05-14T11:00:00+05:30 \
  -end 2026-05-14T19:00:00+05:30 -burst -verbose
```

Config: `config/whale.yaml`
