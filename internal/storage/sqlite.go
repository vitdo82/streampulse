package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure Go SQLite driver
)

// SQLiteStore implements MetricsStore using an embedded SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore creates a new SQLite-backed store.
func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	if dsn == "" {
		dsn = "~/.streampulse/state.db"
	}

	if strings.HasPrefix(dsn, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("home dir: %w", err)
		}
		dsn = filepath.Join(home, dsn[2:])
	}

	dir := filepath.Dir(dsn)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", dir, err)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Performance tuning
	db.SetMaxOpenConns(1) // SQLite: single writer
	db.SetMaxIdleConns(1)

	// WAL mode enables concurrent readers while the daemon writes; both
	// PRAGMAs are per-connection, so set them before any other use (the
	// pool is capped at one connection above). Memory-backed stores have
	// no journal, so skip them.
	if dsn != ":memory:" {
		if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
			return nil, fmt.Errorf("enable WAL: %w", err)
		}
		if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
			return nil, fmt.Errorf("set busy timeout: %w", err)
		}
	}

	store := &SQLiteStore{db: db}
	if err := store.Migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return store, nil
}

func (s *SQLiteStore) WriteBatch(ctx context.Context, metrics []Metric) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO raw_metrics (ts, cluster_id, metric, entity_type, entity_name, tags, value)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range metrics {
		tagsJSON := "{}"
		if len(m.Tags) > 0 {
			b, err := json.Marshal(m.Tags)
			if err != nil {
				return fmt.Errorf("marshal tags for %s/%s: %w", m.EntityType, m.EntityName, err)
			}
			tagsJSON = string(b)
		}

		if _, err := stmt.ExecContext(ctx,
			m.TS.UnixMilli(), m.ClusterID, m.Metric,
			m.EntityType, m.EntityName, tagsJSON, m.Value,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) QueryRaw(ctx context.Context, params QueryParams) ([]MetricRow, error) {
	return nil, nil // TODO: implement
}

func (s *SQLiteStore) QueryHourly(ctx context.Context, params QueryParams) ([]MetricRow, error) {
	return nil, nil // TODO: implement
}

func (s *SQLiteStore) QueryDaily(ctx context.Context, params QueryParams) ([]MetricRow, error) {
	return nil, nil // TODO: implement
}

func (s *SQLiteStore) Rollup(ctx context.Context, resolution string) error {
	return nil // TODO: implement
}

func (s *SQLiteStore) Purge(ctx context.Context, retention Retention) error {
	cutoff := time.Now().Add(-retention.Raw)
	_, err := s.db.ExecContext(ctx, `DELETE FROM raw_metrics WHERE ts < ?`, cutoff.UnixMilli())
	return err
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	// v1: baseline schema (idempotent).
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS raw_metrics (
			ts          INTEGER NOT NULL,
			cluster_id  TEXT NOT NULL,
			metric      TEXT NOT NULL,
			entity_type TEXT NOT NULL,
			entity_name TEXT NOT NULL,
			tags        TEXT DEFAULT '{}',
			value       REAL NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_raw_ts ON raw_metrics(ts);
		CREATE INDEX IF NOT EXISTS idx_raw_entity ON raw_metrics(cluster_id, metric, entity_type, entity_name, ts);

		CREATE TABLE IF NOT EXISTS alert_state (
			rule_name    TEXT PRIMARY KEY,
			status       TEXT DEFAULT 'ok',
			last_fired   INTEGER,
			last_value   REAL,
			notify_count INTEGER DEFAULT 0
		);
	`); err != nil {
		return fmt.Errorf("migrate v1: %w", err)
	}

	// v2: aggregate tables for hourly/daily rollup, guarded by user_version.
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version < 2 {
		if _, err := s.db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS hourly_metrics (
				bucket      INTEGER NOT NULL,   -- truncated-to-hour unix ms
				cluster_id  TEXT NOT NULL,
				metric      TEXT NOT NULL,
				entity_type TEXT NOT NULL,
				entity_name TEXT NOT NULL,
				tags        TEXT NOT NULL DEFAULT '{}',
				avg REAL, min REAL, max REAL,
				p50 REAL, p95 REAL, p99 REAL,
				count INTEGER, sum REAL,
				PRIMARY KEY (bucket, cluster_id, metric, entity_type, entity_name, tags)
			);
			CREATE TABLE IF NOT EXISTS daily_metrics (
				bucket      INTEGER NOT NULL,   -- truncated-to-day unix ms
				cluster_id  TEXT NOT NULL,
				metric      TEXT NOT NULL,
				entity_type TEXT NOT NULL,
				entity_name TEXT NOT NULL,
				tags        TEXT NOT NULL DEFAULT '{}',
				avg REAL, min REAL, max REAL,
				p50 REAL, p95 REAL, p99 REAL,
				count INTEGER, sum REAL,
				PRIMARY KEY (bucket, cluster_id, metric, entity_type, entity_name, tags)
			);
			CREATE INDEX IF NOT EXISTS idx_hourly_lookup ON hourly_metrics(cluster_id, metric, entity_type, entity_name, bucket);
			CREATE INDEX IF NOT EXISTS idx_daily_lookup  ON daily_metrics(cluster_id, metric, entity_type, entity_name, bucket);
			PRAGMA user_version = 2;
		`); err != nil {
			return fmt.Errorf("migrate v2: %w", err)
		}
	}

	return nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// Compile-time check that SQLiteStore implements MetricsStore.
var _ MetricsStore = (*SQLiteStore)(nil)
