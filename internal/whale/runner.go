package whale

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"crypto_announcements_go/internal/binance"
)

type Runner struct {
	cfg    Config
	client *binance.FuturesClient
	ws     *FuturesWS
	exec    *Executor
	journal *TradeJournal

	flash    sync.Map
	bookLead sync.Map
	burst    sync.Map

	tradesProcessed atomic.Uint64
	burstsFired     atomic.Uint64
}

func (r *Runner) TradeLogPath() string {
	if r == nil || r.journal == nil {
		return ""
	}
	return r.journal.Path()
}

func NewRunner(cfg Config, client *binance.FuturesClient) (*Runner, error) {
	journal, err := NewTradeJournal(cfg.TradeLogPath)
	if err != nil {
		return nil, err
	}
	return &Runner{
		cfg:    cfg,
		client: client,
		ws:     NewFuturesWS(cfg),
		exec:   NewExecutor(cfg, client, journal),
		journal: journal,
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	for _, sym := range r.cfg.Symbols {
		if r.cfg.UsesBookLead() {
			r.bookLead.Store(sym, NewBookLeadDetector(r.cfg.BookLead))
		}
		if r.cfg.UsesFlash() {
			r.flash.Store(sym, NewFlashDetector(r.cfg.Flash))
		}
		if r.cfg.UsesBurst() {
			r.burst.Store(sym, NewBurstDetector(r.cfg.Burst))
		}
	}
	if r.cfg.UsesBurst() {
		b := r.cfg.Burst
		mode := "burst"
		if b.PumpOnly {
			mode = "burst pump-only"
		}
		log.Printf("[whale] %s | symbols=%d | 100ms>=%.2f%% | $1s>=%.0f | cooldown=%.0fs | mega tp=%.0f%%",
			mode, len(r.cfg.Symbols), b.MinFastMovePct, b.MinSecNotionalUSDT, r.cfg.CooldownSec,
			r.cfg.Risk.MegaTakeProfitPct)
	}
	if r.cfg.UsesBookLead() {
		bl := r.cfg.BookLead
		moveCap := "off"
		if bl.MaxEntryMovePct > 0 {
			moveCap = fmt.Sprintf("<%.2f%%", bl.MaxEntryMovePct)
		}
		thin := "off"
		if bl.MaxThinSideUSDT > 0 {
			thin = formatUSDT(bl.MaxThinSideUSDT)
		}
		imb := "off"
		if bl.MinImbalanceRatio > 0 {
			imb = fmt.Sprintf("%.1fx", bl.MinImbalanceRatio)
		}
		log.Printf("[whale] book-lead detector | symbols=%d | window=%dms | imb=%s | thin=%s | flow>$%.0f dom=%.0f%% | move_cap=%s",
			len(r.cfg.Symbols), bl.WindowMs, imb, thin, bl.MinTradeNotionalUSDT, bl.TradeDominancePct, moveCap)
	}
	if r.cfg.UsesFlash() {
		log.Printf("[whale] flash detector | symbols=%d | 1s>=%.1f%% early>=%.1f%%+100ms>=%.2f%%",
			len(r.cfg.Symbols), r.cfg.Flash.MinSecMovePct, r.cfg.Flash.EarlySecMovePct, r.cfg.Flash.MinFastMovePct)
	}

	workers := runtime.NumCPU() * 2
	if workers < 4 {
		workers = 4
	}
	if workers > 32 {
		workers = 32
	}
	log.Printf("[whale] event workers=%d", workers)
	for i := 0; i < workers; i++ {
		go r.eventWorker(ctx)
	}
	go r.statsHeartbeat(ctx)
	return r.ws.Run(ctx)
}

func (r *Runner) statsHeartbeat(ctx context.Context) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tr := r.tradesProcessed.Swap(0)
			bu := r.burstsFired.Swap(0)
			recv, enq, drop := r.ws.ConsumeStats()
			log.Printf("[whale] health 1h: ws_recv=%d enqueued=%d dropped=%d trades=%d bursts=%d",
				recv, enq, drop, tr, bu)
		}
	}
}

func formatUSDT(v float64) string {
	if v >= 1_000_000 {
		return strconv.FormatFloat(v/1_000_000, 'f', 1, 64) + "M"
	}
	if v >= 1000 {
		return strconv.FormatFloat(v/1000, 'f', 0, 64) + "k"
	}
	return strconv.FormatFloat(v, 'f', 0, 64)
}

func (r *Runner) eventWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-r.ws.Events():
			if !ok {
				return
			}
			r.processEvent(ev)
		}
	}
}

func (r *Runner) processEvent(ev StreamEvent) {
	sym := ev.Symbol
	if ev.Trade != nil {
		r.processTrade(sym, ev)
	}
	if len(ev.Bids) > 0 && len(ev.Asks) > 0 {
		r.processDepth(sym, ev)
	}
}

func (r *Runner) processTrade(sym string, ev StreamEvent) {
	price, qty, buyerMaker := parseAggTrade(ev.Trade)
	if price <= 0 || qty <= 0 {
		return
	}
	r.tradesProcessed.Add(1)
	tradeAt := tradeEventTime(ev.Recv, ev.Trade)

	if det, ok := r.bookLeadDet(sym); ok {
		if sig := det.OnAggTrade(price, qty, buyerMaker, tradeAt); sig != nil {
			sig.Symbol = sym
			sig.EntryPrice = price
			sig.RecvAt = ev.Recv
			r.dispatchSignal(sig)
		}
	}
	if det, ok := r.flashDet(sym); ok {
		if sig := det.OnAggTrade(price, qty, buyerMaker, tradeAt); sig != nil {
			sig.Symbol = sym
			sig.EntryPrice = price
			sig.RecvAt = ev.Recv
			r.dispatchSignal(sig)
		}
	}
	if det, ok := r.burstDet(sym); ok {
		if sig := det.OnAggTrade(price, qty, buyerMaker, tradeAt); sig != nil {
			r.burstsFired.Add(1)
			sig.Symbol = sym
			sig.EntryPrice = price
			sig.Mega = true
			sig.RecvAt = ev.Recv
			r.dispatchSignal(sig)
		}
	}
	if r.cfg.DryRun && r.cfg.UsesTickDrySim() {
		r.exec.OnPriceTick(sym, price, tradeAt)
	}
}

func (r *Runner) processDepth(sym string, ev StreamEvent) {
	det, ok := r.bookLeadDet(sym)
	if !ok {
		return
	}
	sig := det.OnDepth(ev.Bids, ev.Asks, ev.Recv)
	if sig == nil {
		return
	}
	sig.Symbol = sym
	r.dispatchSignal(sig)
}

func (r *Runner) dispatchSignal(sig *Signal) {
	go r.exec.HandleSignal(context.Background(), sig)
}

func (r *Runner) flashDet(sym string) (*FlashDetector, bool) {
	v, ok := r.flash.Load(sym)
	if !ok {
		return nil, false
	}
	return v.(*FlashDetector), true
}

func (r *Runner) burstDet(sym string) (*BurstDetector, bool) {
	v, ok := r.burst.Load(sym)
	if !ok {
		return nil, false
	}
	return v.(*BurstDetector), true
}

func (r *Runner) bookLeadDet(sym string) (*BookLeadDetector, bool) {
	v, ok := r.bookLead.Load(sym)
	if !ok {
		return nil, false
	}
	return v.(*BookLeadDetector), true
}

func parseAggTrade(ev *aggTradeEvent) (price, qty float64, buyerIsMaker bool) {
	if ev == nil {
		return 0, 0, false
	}
	price, _ = strconv.ParseFloat(ev.Price, 64)
	qty, _ = strconv.ParseFloat(ev.Quantity, 64)
	return price, qty, ev.Maker
}
