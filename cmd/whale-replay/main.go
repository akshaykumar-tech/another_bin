package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"

	"crypto_announcements_go/internal/binance"
	"crypto_announcements_go/internal/whale"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	symbol := flag.String("symbol", "AIGENSYNUSDT", "futures symbol")
	startStr := flag.String("start", "", "start time RFC3339 (default: last Thu 11:00 IST)")
	endStr := flag.String("end", "", "end time RFC3339 (default: last Thu 19:00 IST)")
	cfgPath := flag.String("config", "config/whale.yaml", "whale config")
	pnlOnly := flag.Bool("pnl", false, "booklead PnL backtest only (synthetic book from flow)")
	burstPnL := flag.Bool("burst", false, "burst strategy PnL + 14 May pump capture report")
	verbose := flag.Bool("verbose", false, "log each entry/exit")
	flag.Parse()

	loc, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(loc)
	lastThu := lastThursday(now)
	start := time.Date(lastThu.Year(), lastThu.Month(), lastThu.Day(), 11, 0, 0, 0, loc)
	end := time.Date(lastThu.Year(), lastThu.Month(), lastThu.Day(), 19, 0, 0, 0, loc)
	if *startStr != "" {
		t, err := time.Parse(time.RFC3339, *startStr)
		if err != nil {
			log.Fatalf("start: %v", err)
		}
		start = t
	}
	if *endStr != "" {
		t, err := time.Parse(time.RFC3339, *endStr)
		if err != nil {
			log.Fatalf("end: %v", err)
		}
		end = t
	}

	cfg, err := whale.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	client := binance.NewFuturesClient("https://fapi.binance.com", os.Getenv("BINANCE_API_KEY"), os.Getenv("BINANCE_API_SECRET"))
	if err := client.WarmSymbolCache(); err != nil {
		log.Fatalf("exchangeInfo: %v", err)
	}
	if !client.SymbolTradable(*symbol) {
		log.Printf("[replay] warning: %s not active on USDT-M now — loading historical aggTrades anyway", *symbol)
	}

	startUTC := start.UTC()
	endUTC := end.UTC()
	log.Printf("[replay] symbol=%s window=%s → %s (IST %s → %s)",
		*symbol, startUTC.Format(time.RFC3339), endUTC.Format(time.RFC3339),
		start.Format("2006-01-02 15:04"), end.Format("2006-01-02 15:04"))

	trades, err := client.FetchAggTradesRange(*symbol, startUTC, endUTC)
	if err != nil {
		log.Fatalf("fetch aggTrades: %v", err)
	}
	log.Printf("[replay] loaded %d aggTrades", len(trades))
	if len(trades) == 0 {
		log.Printf("[replay] no trades in window — check date/symbol")
		return
	}

	if *burstPnL {
		runBurstBacktest(cfg, *symbol, trades, *verbose)
		return
	}
	if *pnlOnly {
		runPnLBacktest(cfg, client, *symbol, trades, *verbose)
		return
	}

	printLargestMoves(trades, 1000)
	replayFlash(*symbol, cfg.Flash, trades)
	replayBookLead(*symbol, cfg.BookLead, trades)
	replayFlowNearMisses(*symbol, cfg.BookLead, trades)
	runPnLBacktest(cfg, client, *symbol, trades, *verbose)
}

func runBurstBacktest(cfg whale.Config, symbol string, trades []binance.AggTrade, verbose bool) {
	ist := mustIST()
	// 14 May 2026 pump ~15:30 IST
	pumpFrom := time.Date(2026, 5, 14, 15, 29, 30, 0, ist)
	pumpTo := time.Date(2026, 5, 14, 15, 31, 30, 0, ist)
	warmup := time.Date(2026, 5, 14, 15, 25, 0, 0, ist)
	pumpEnd := time.Date(2026, 5, 14, 15, 40, 0, 0, ist)

	log.Printf("[replay] ===== BURST full day PnL =====")
	sum := whale.RunBurstBacktest(cfg, nil, symbol, trades, verbose)
	log.Printf("[replay] signals=%d entries=%d PnL=%+.2f USDT (SL=%d trail/tp=%d timeout=%d)",
		sum.Signals, sum.Entries, sum.TotalPnLUSDT, sum.ExitsSL, sum.ExitsTP2, sum.ExitsTimeout)

	log.Printf("[replay] ===== PUMP 14 May 15:30 IST (mega trail) =====")
	rep := whale.ReplayBurstPump(cfg, symbol, trades, warmup, pumpFrom, pumpTo, pumpEnd)
	if !rep.Found {
		log.Printf("[replay] no burst entry in pump window 15:29:30–15:31:30 IST")
		return
	}
	log.Printf("[replay] entry %s IST @ %.6f", rep.EntryTime.In(ist).Format("15:04:05.000"), rep.EntryPrice)
	log.Printf("[replay] exit  %s IST @ %.6f reason=%s", rep.ExitTime.In(ist).Format("15:04:05.000"), rep.ExitPrice, rep.ExitReason)
	log.Printf("[replay] CAPTURE %.2f%% of move | PEAK after entry %.2f%% | signals in window=%d",
		rep.CapturePct, rep.PeakPct, rep.Signals)
	if rep.CapturePct >= 10 {
		log.Printf("[replay] target met: >=10%% capture on pump trade")
	} else {
		log.Printf("[replay] below 10%% capture target (entry timing / trail / exit)")
	}
}

func runPnLBacktest(cfg whale.Config, client *binance.FuturesClient, symbol string, trades []binance.AggTrade, verbose bool) {
	log.Printf("[replay] ===== BOOKLEAD PnL (synthetic book from flow) =====")
	sum := whale.RunBookLeadBacktest(cfg, client, symbol, trades, verbose)
	log.Printf("[replay] trades=%d signals=%d entries=%d skipped_cd=%d skipped_pos=%d",
		sum.TradesLoaded, sum.Signals, sum.Entries, sum.SkippedCooldown, sum.SkippedMaxPos)
	log.Printf("[replay] exits: SL=%d TP1=%d TP2=%d timeout=%d",
		sum.ExitsSL, sum.ExitsTP1, sum.ExitsTP2, sum.ExitsTimeout)
	result := "LOSS"
	if sum.TotalPnLUSDT > 0 {
		result = "PROFIT"
	} else if sum.TotalPnLUSDT == 0 && sum.Entries == 0 {
		result = "NO_TRADES"
	}
	log.Printf("[replay] PnL: %+.2f USDT | capital=%.0f | %s",
		sum.TotalPnLUSDT, cfg.CapitalUSDT, result)
}

func lastThursday(t time.Time) time.Time {
	d := t
	for d.Weekday() != time.Thursday {
		d = d.AddDate(0, 0, -1)
	}
	return d
}

type moveWindow struct {
	at    time.Time
	pct   float64
	p0, p1 float64
	vol   float64
}

func printLargestMoves(trades []binance.AggTrade, windowMs int) {
	if len(trades) < 2 {
		return
	}
	w := time.Duration(windowMs) * time.Millisecond
	var best []moveWindow
	for i := 0; i < len(trades); i++ {
		p0 := trades[i].Price
		if p0 <= 0 {
			continue
		}
		var vol float64
		end := trades[i].Time
		for j := i; j < len(trades) && !trades[j].Time.After(end.Add(w)); j++ {
			vol += trades[j].Price * trades[j].Quantity
		}
		j := i
		for j < len(trades) && !trades[j].Time.After(end.Add(w)) {
			p1 := trades[j].Price
			pct := (p1 - p0) / p0 * 100
			if math.Abs(pct) >= 0.5 {
				best = append(best, moveWindow{at: trades[j].Time, pct: pct, p0: p0, p1: p1, vol: vol})
			}
			j++
		}
	}
	sort.Slice(best, func(i, j int) bool {
		return math.Abs(best[i].pct) > math.Abs(best[j].pct)
	})
	log.Printf("[replay] top 1s moves (window=%dms):", windowMs)
	limit := 10
	if len(best) < limit {
		limit = len(best)
	}
	seen := make(map[string]bool)
	for _, m := range best {
		if limit <= 0 {
			break
		}
		key := m.at.Format("15:04:05") + fmt.Sprintf("%.1f", m.pct)
		if seen[key] {
			continue
		}
		seen[key] = true
		ist := m.at.In(mustIST())
		log.Printf("[replay]   %s IST | %s UTC | move=%+.2f%% | $vol1s=%.0f | %.6f→%.6f",
			ist.Format("15:04:05"), m.at.UTC().Format("15:04:05"), m.pct, m.vol, m.p0, m.p1)
		limit--
	}
}

func mustIST() *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return time.FixedZone("IST", 5*3600+30*60)
	}
	return loc
}

func replayFlash(symbol string, cfg whale.FlashConfig, trades []binance.AggTrade) {
	det := whale.NewFlashDetector(cfg)
	var signals int
	for _, tr := range trades {
		sig := det.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
		if sig == nil {
			continue
		}
		signals++
		ist := tr.Time.In(mustIST())
		log.Printf("[replay] FLASH signal #%d | %s %s | mode=%s | 1s=%.2f%% 100ms=%.2f%% vol=$%.0f | %s IST",
			signals, sig.Side, symbol, sig.FlashMode, sig.MovePct, sig.FastMove, sig.SecVolume, ist.Format("2006-01-02 15:04:05"))
	}
	log.Printf("[replay] FLASH total signals=%d (min_sec_move=%.1f%% min_vol=$%.0f)", signals, cfg.MinSecMovePct, cfg.MinSecNotionalUSDT)
}

func replayBookLead(symbol string, cfg whale.BookLeadConfig, trades []binance.AggTrade) {
	det := whale.NewBookLeadDetector(cfg)
	var signals int
	for _, tr := range trades {
		sig := det.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time)
		if sig == nil {
			continue
		}
		signals++
		ist := tr.Time.In(mustIST())
		log.Printf("[replay] BOOK signal #%d | %s %s | mode=%s | imb=%.2fx thin=$%.0f flow=$%.0f move=%.2f%% | %s IST",
			signals, sig.Side, symbol, sig.BookMode, sig.ImbalanceRatio, sig.ThinSideUSDT, sig.TradeFlowUSDT, sig.MovePct, ist.Format("2006-01-02 15:04:05"))
	}
	log.Printf("[replay] BOOK total signals=%d (needs live depth — aggTrade-only replay cannot set bid/ask book)", signals)
}

func replayFlowNearMisses(symbol string, cfg whale.BookLeadConfig, trades []binance.AggTrade) {
	w := time.Duration(cfg.WindowMs) * time.Millisecond
	dom := cfg.TradeDominancePct / 100
	var near int
	for i := 0; i < len(trades); i++ {
		end := trades[i].Time
		var buy, sell float64
		var p0, p1 float64
		p0 = trades[i].Price
		p1 = p0
		for j := i; j < len(trades) && !trades[j].Time.After(end.Add(w)); j++ {
			v := trades[j].Price * trades[j].Quantity
			if trades[j].BuyerIsMaker {
				sell += v
			} else {
				buy += v
			}
			p1 = trades[j].Price
		}
		total := buy + sell
		if total < cfg.MinTradeNotionalUSDT {
			continue
		}
		move := 0.0
		if p0 > 0 {
			move = (p1 - p0) / p0 * 100
		}
		if cfg.MaxEntryMovePct > 0 && math.Abs(move) > cfg.MaxEntryMovePct {
			continue
		}
		longFlow := buy >= total*dom && buy >= cfg.MinTradeNotionalUSDT
		shortFlow := sell >= total*dom && sell >= cfg.MinTradeNotionalUSDT
		if !longFlow && !shortFlow {
			continue
		}
		near++
		if near <= 15 {
			side := "BUY"
			flow := buy
			if shortFlow {
				side = "SELL"
				flow = sell
			}
			ist := end.In(mustIST())
			log.Printf("[replay] FLOW-ONLY (no book) #%d | %s | vol=$%.0f flow=$%.0f move=%.2f%% | %s IST — book rules not tested",
				near, side, total, flow, move, ist.Format("2006-01-02 15:04:05"))
		}
	}
	log.Printf("[replay] FLOW-ONLY windows matching volume rules=%d (book imbalance/thin not in historical data)", near)
}
