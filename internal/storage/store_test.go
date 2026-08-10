package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreUnsupportedBackends(t *testing.T) {
	for _, typ := range []string{"postgres", "clickhouse"} {
		store, err := NewStore(typ, "")
		require.Error(t, err, typ)
		assert.Nil(t, store, typ)
	}
}

func TestNewStoreDefaultsToSQLite(t *testing.T) {
	store, err := NewStore("", ":memory:")
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()
}
