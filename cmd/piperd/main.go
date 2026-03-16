package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tjstebbing/piperdb/internal/server"
	"github.com/tjstebbing/piperdb/pkg/db"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data-dir", "", "data directory (default: ./data, or PIPERDB_DATA_DIR)")
	flag.Parse()

	dir := *dataDir
	if dir == "" {
		dir = os.Getenv("PIPERDB_DATA_DIR")
	}
	if dir == "" {
		dir = "./data"
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	cfg := db.DefaultConfig()
	cfg.DataDir = dir

	database, err := db.Open(cfg)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := server.New(database, *addr)
	if err := srv.StartWithContext(ctx); err != nil && err.Error() != "http: Server closed" {
		log.Fatalf("Server error: %v", err)
	}
}
