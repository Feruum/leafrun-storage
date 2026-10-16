package main

import (
	"context"
	"fmt"
	"github.com/leafrun/leafrun-storage/internal/storage"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("storage stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: leafrun-storage api|worker|migrate|probe-s3|event-relay")
	}
	if os.Args[1] == "event-relay" {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		return storage.RunAuditRelay(ctx, os.Getenv)
	}
	cfg, e := storage.LoadConfig()
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	blob, e := storage.OpenS3(ctx, cfg)
	if e != nil {
		return e
	}
	if os.Args[1] == "probe-s3" {
		return blob.VerifyCapabilities(ctx)
	}
	db, e := storage.OpenDB(ctx, cfg.DatabaseURL)
	if e != nil {
		return e
	}
	defer db.Pool.Close()
	if os.Args[1] == "migrate" {
		return db.Migrate(ctx)
	}
	if e = blob.VerifyCapabilities(ctx); e != nil {
		return e
	}
	s := &storage.Service{DB: db, Blob: blob, Owner: storage.NewOwner(cfg.OwnerURL, cfg.OwnerToken), Config: cfg}
	switch os.Args[1] {
	case "worker":
		return s.RunWorker(ctx)
	case "api":
		server := &http.Server{Addr: cfg.Listen, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}
		go func() {
			<-ctx.Done()
			shutdown, c := context.WithTimeout(context.Background(), 10*time.Second)
			defer c()
			_ = server.Shutdown(shutdown)
		}()
		slog.Info("storage API listening", "listen", cfg.Listen)
		e = server.ListenAndServe()
		if e == http.ErrServerClosed {
			return nil
		}
		return e
	default:
		return fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
}
