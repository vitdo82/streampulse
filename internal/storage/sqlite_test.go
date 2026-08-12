package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateVersionedSchema(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	require.NoError(t, err)
	defer s.Close()

	var version int
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&version))
	assert.Equal(t, 2, version, "fresh DB must be migrated to schema version 2")

	for _, table := range []string{"raw_metrics", "alert_state", "hourly_metrics", "daily_metrics"} {
		var sqlText string
		require.NoError(t, s.db.QueryRow(
			`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
		).Scan(&sqlText), "table %s must exist", table)
		if table == "hourly_metrics" || table == "daily_metrics" {
			assert.Contains(t, sqlText, "PRIMARY KEY", "table %s must declare a PRIMARY KEY", table)
		}
	}

	for _, idx := range []string{"idx_raw_ts", "idx_raw_entity", "idx_hourly_lookup", "idx_daily_lookup"} {
		var name string
		require.NoError(t, s.db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, idx,
		).Scan(&name), "index %s must exist", idx)
	}

	// A second Migrate call is a no-op and must not reset the version.
	require.NoError(t, s.Migrate(context.Background()))
	require.NoError(t, s.db.QueryRow(`PRAGMA user_version`).Scan(&version))
	assert.Equal(t, 2, version)
}

func TestNewSQLiteStoreWalMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := NewSQLiteStore(path)
	require.NoError(t, err)
	defer s.Close()

	var mode string
	require.NoError(t, s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode))
	assert.Equal(t, "wal", mode, "file-backed store must run in WAL mode")
}
