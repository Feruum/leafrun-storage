package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/leafrun/leafrun-platform/events"
)

var storageMigrationFiles = []string{"migrations/0001_catalog.sql", "migrations/0002_pin_reconciliation_fairness.sql", "migrations/0003_gc_retry_fairness.sql", "migrations/0004_e06_audit.sql"}

func storageChecksum(i int) string {
	raw, _ := migrations.ReadFile(storageMigrationFiles[i])
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type migrationQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func storageLedger(ctx context.Context, q migrationQuerier) (int, error) {
	rows, err := q.Query(ctx, `SELECT version,checksum FROM storage_schema_migrations ORDER BY version`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var version int
		var sum string
		if rows.Scan(&version, &sum) != nil || n >= len(storageMigrationFiles) || version != n+1 || sum != storageChecksum(n) {
			return 0, errors.New("Storage migration ledger mismatch")
		}
		n++
	}
	return n, rows.Err()
}
func (d *DB) Migrate(ctx context.Context) error {
	if err := events.ApplyMigration(ctx, d.Pool); err != nil {
		return err
	}
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(710032);CREATE TABLE IF NOT EXISTS storage_schema_migrations(version integer PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	n, err := storageLedger(ctx, tx)
	if err != nil {
		return err
	}
	// Prior Storage schemas used idempotent embedded SQL without a ledger. The
	// first upgrade reruns those exact historical bytes and records their hashes.
	for i := n; i < len(storageMigrationFiles); i++ {
		raw, e := migrations.ReadFile(storageMigrationFiles[i])
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, string(raw)); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `INSERT INTO storage_schema_migrations(version,checksum)VALUES($1,$2)`, i+1, storageChecksum(i)); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
