package whale

import (
	"log"
	"sync"
	"time"
)

// FocusController pauses all symbols except the active trade; 2m cooldown after exit.
type FocusController struct {
	mu          sync.RWMutex
	focusSymbol string
	pauseUntil  time.Time
	cooldown    time.Duration
}

func NewFocusController(cooldownSec float64) *FocusController {
	d := time.Duration(cooldownSec * float64(time.Second))
	if d <= 0 {
		d = 2 * time.Minute
	}
	return &FocusController{cooldown: d}
}

func (f *FocusController) AllowsEvent(sym string) bool {
	if f == nil {
		return true
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.pauseUntil.IsZero() && time.Now().Before(f.pauseUntil) {
		return false
	}
	if f.focusSymbol != "" && f.focusSymbol != sym {
		return false
	}
	return true
}

func (f *FocusController) TryBegin(sym string) bool {
	if f == nil {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.pauseUntil.IsZero() && time.Now().Before(f.pauseUntil) {
		return false
	}
	if f.focusSymbol != "" {
		return f.focusSymbol == sym
	}
	f.focusSymbol = sym
	log.Printf("[whale] focus ON %s — other symbols paused until trade ends", sym)
	return true
}

func (f *FocusController) ReleaseWithoutTrade(sym string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.focusSymbol == sym {
		f.focusSymbol = ""
		log.Printf("[whale] focus OFF %s (no trade opened)", sym)
	}
}

func (f *FocusController) EndTradeCooldown(sym string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.focusSymbol != sym {
		return
	}
	f.focusSymbol = ""
	f.pauseUntil = time.Now().Add(f.cooldown)
	log.Printf("[whale] focus OFF %s — all symbols paused %s (resume ~%s)",
		sym, f.cooldown.Round(time.Second), f.pauseUntil.UTC().Format("15:04:05"))
}

func (f *FocusController) InCooldown() bool {
	if f == nil {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return !f.pauseUntil.IsZero() && time.Now().Before(f.pauseUntil)
}
