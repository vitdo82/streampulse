package storage

import (
	"context"
	"database/sql"
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
			// Simplified: real implementation uses encoding/json
			tagsJSON = fmt.Sprintf("%v", m.Tags)
		}

		if _, err := stmt.ExecContext(ctx,
			m.TS.Unix(), m.ClusterID, m.Metric,
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
	_, err := s.db.ExecContext(ctx, `DELETE FROM raw_metrics WHERE ts < ?`, cutoff.Unix())
	return err
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
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
	`)
	return err
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// Compile-time check that SQLiteStore implements MetricsStore.
var _ MetricsStore = (*SQLiteStore)(nil)
