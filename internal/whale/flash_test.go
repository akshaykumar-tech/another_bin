package whale

import (
	"testing"
	"time"
)

func TestFlashFullMove(t *testing.T) {
	cfg := DefaultConfig().Flash
	cfg.MinSecMovePct = 5
	cfg.MinSecNotionalUSDT = 1000
	cfg.MinFastNotionalUSDT = 100
	d := NewFlashDetector(cfg)
	now := time.Now()
	for i := 0; i < 20; i++ {
		p := 1.0 + float64(i)*0.003
		d.OnAggTrade(p, 5000, false, now.Add(time.Duration(i)*50*time.Millisecond))
	}
	sig := d.OnAggTrade(1.10, 8000, false, now.Add(950*time.Millisecond))
	if sig == nil {
		t.Fatal("expected flash on ~10% 1s move")
	}
	if sig.FlashMode != "full" {
		t.Fatalf("mode=%s", sig.FlashMode)
	}
}
