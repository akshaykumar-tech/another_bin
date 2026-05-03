package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"crypto_announcements_go/internal/app"
	"crypto_announcements_go/internal/config"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Best-effort .env loading so local runs work without manual export.
	if err := godotenv.Load(); err != nil {
		log.Printf("[config] .env not loaded: %v", err)
	}

	cfg := config.Load()
	svc, err := app.New(cfg)
	if err != nil {
		log.Fatalf("boot failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := svc.Run(ctx); err != nil {
		log.Fatalf("runtime failed: %v", err)
	}
}
