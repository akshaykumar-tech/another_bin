package whale

import (
	"log"
	"strconv"
	"time"

	"crypto_announcements_go/internal/binance"
)

// RunBookLeadBacktest replays aggTrades with synthetic depth from trade flow
// (no historical L2 from Binance). PnL uses the same SL/TP as live executor.
func RunBookLeadBacktest(cfg Config, client *binance.FuturesClient, symbol string, trades []binance.AggTrade, verbose bool) BacktestSummary {
	st := &bookLeadReplayState{
		replayState: replayState{
			cfg:       cfg,
			client:    client,
			open:      make(map[string]*simPosition),
			lastTrade: make(map[string]time.Time),
			verbose:   verbose,
		},
		book: NewBookLeadDetector(cfg.BookLead),
	}
	st.summary.Symbols = 1
	win := time.Duration(cfg.BookLead.WindowMs) * time.Millisecond

	for _, tr := range trades {
		st.summary.TradesLoaded++
		st.markToMarket(symbol, tr.Price, tr.Time)

		v := tr.Price * tr.Quantity
		st.buf = append(st.buf, flowTick{at: tr.Time, buy: !tr.BuyerIsMaker, vol: v})
		cut := tr.Time.Add(-win)
		i := 0
		for i < len(st.buf) && st.buf[i].at.Before(cut) {
			i++
		}
		if i > 0 {
			st.buf = st.buf[i:]
		}

		var buy, sell float64
		for _, t := range st.buf {
			if t.buy {
				buy += t.vol
			} else {
				sell += t.vol
			}
		}

		bids, asks := syntheticBook(tr.Price, buy, sell, cfg.BookLead.MinImbalanceRatio)
		if sig := st.book.OnDepth(bids, asks, tr.Time); sig != nil {
			sig.Symbol = symbol
			st.dispatchBook(sig, tr.Price)
		}
		if sig := st.book.OnAggTrade(tr.Price, tr.Quantity, tr.BuyerIsMaker, tr.Time); sig != nil {
			sig.Symbol = symbol
			st.dispatchBook(sig, tr.Price)
		}
	}

	end := time.Time{}
	if len(trades) > 0 {
		end = trades[len(trades)-1].Time
	}
	if pos, ok := st.open[symbol]; ok {
		px := trades[len(trades)-1].Price
		st.closePosition(symbol, pos, px, end, "timeout")
	}
	return st.summary
}

type flowTick struct {
	at  time.Time
	buy bool
	vol float64
}

type bookLeadReplayState struct {
	replayState
	book *BookLeadDetector
	buf  []flowTick
}

func (st *bookLeadReplayState) dispatchBook(sig *Signal, entry float64) {
	st.summary.Signals++
	if st.openCount >= st.cfg.MaxOpenPositions {
		st.summary.SkippedMaxPos++
		return
	}
	if t, ok := st.lastTrade[sig.Symbol]; ok && sig.RecvAt.Sub(t) < st.cfg.Cooldown() {
		st.summary.SkippedCooldown++
		return
	}
	if _, busy := st.open[sig.Symbol]; busy {
		st.summary.SkippedCooldown++
		return
	}
	margin := st.marginFor(sig)
	if entry <= 0 {
		return
	}
	if st.verbose {
		log.Printf("[backtest] BOOK %s %s mode=%s move=%.2f%% imb=%.2fx flow=$%.0f",
			sig.Side, sig.Symbol, sig.BookMode, sig.MovePct, sig.ImbalanceRatio, sig.TradeFlowUSDT)
		log.Printf("[backtest] DRY_RUN %s %s margin=%.2f entry=%.6f", sig.Side, sig.Symbol, margin, entry)
	}
	st.open[sig.Symbol] = &simPosition{
		Symbol: sig.Symbol, Side: sig.Side, EntryPrice: entry, MarginUSDT: margin, OpenedAt: sig.RecvAt,
	}
	st.openCount++
	st.lastTrade[sig.Symbol] = sig.RecvAt
	st.summary.Entries++
}

// syntheticBook builds L2 levels from flow so replay can exercise booklead rules.
func syntheticBook(mid, buyFlow, sellFlow, ratioMin float64) (bids, asks [][]string) {
	if mid <= 0 {
		return nil, nil
	}
	const minN = 5000.0
	if buyFlow >= sellFlow {
		bidN := buyFlow * 2
		if bidN < minN {
			bidN = minN
		}
		askN := sellFlow
		if askN < 100 {
			askN = 100
		}
		if ratioMin > 0 && bidN < askN*ratioMin {
			bidN = askN * ratioMin * 1.05
		}
		return levels(mid, bidN, askN, true)
	}
	askN := sellFlow * 2
	if askN < minN {
		askN = minN
	}
	bidN := buyFlow
	if bidN < 100 {
		bidN = 100
	}
	if ratioMin > 0 && askN < bidN*ratioMin {
		askN = bidN * ratioMin * 1.05
	}
	return levels(mid, bidN, askN, false)
}

func levels(mid, bidN, askN float64, longBias bool) ([][]string, [][]string) {
	bidP := mid * 0.9995
	askP := mid * 1.0005
	bidQ := bidN / bidP
	askQ := askN / askP
	if longBias {
		return [][]string{{fmtPrice(bidP), fmtQty(bidQ)}}, [][]string{{fmtPrice(askP), fmtQty(askQ)}}
	}
	return [][]string{{fmtPrice(bidP), fmtQty(bidQ)}}, [][]string{{fmtPrice(askP), fmtQty(askQ)}}
}

func fmtPrice(p float64) string {
	return strconv.FormatFloat(p, 'f', 8, 64)
}

func fmtQty(q float64) string {
	return strconv.FormatFloat(q, 'f', 2, 64)
}
