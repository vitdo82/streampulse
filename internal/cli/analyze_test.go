package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedGrowthStore writes 3 days of kafka.topic.messages raw points and rolls
// them up so QueryDaily returns daily aggregates for the given topics.
func seedGrowthStore(t *testing.T, path string, topics map[string]float64) {
	t.Helper()
	store, err := storage.NewSQLiteStore(path)
	require.NoError(t, err)
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Hour)
	var batch []storage.Metric
	day := 0
	for d := 2; d >= 0; d-- {
		for h := 0; h < 12; h++ {
			for name, rate := range topics {
				batch = append(batch, storage.Metric{
					TS: now.Add(-time.Duration(d)*24*time.Hour + time.Duration(h)*2*time.Hour),
					ClusterID: "local-dev", Metric: scraper.MetricTopicMessages,
					EntityType: "topic", EntityName: name, Value: rate * float64(h+1),
				})
			}
		}
		day++
	}
	require.NoError(t, store.WriteBatch(context.Background(), batch))
	require.NoError(t, store.Rollup(context.Background(), "hourly"))
	require.NoError(t, store.Rollup(context.Background(), "daily"))
}

func TestAnalyzePrintsGrowthSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	seedGrowthStore(t, path, map[string]float64{"orders": 100, "payments": 50})
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze", "--window", "24h", "--topics", "orders"})
	require.NoError(t, root.Execute())

	out := buf.String()
	assert.Contains(t, out, "Growth")
	assert.Contains(t, out, "orders")
	assert.Contains(t, out, "▁") // sparkline glyphs
}

func TestAnalyzeJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	seedGrowthStore(t, path, map[string]float64{"orders": 100})
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", path)

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze", "--window", "24h", "--json"})
	require.NoError(t, root.Execute())

	var out struct {
		Growth []struct {
			Topic     string          `json:"topic"`
			Window    json.RawMessage `json:"window"`
			Sparkline string          `json:"sparkline"`
		} `json:"growth"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out))
	require.Len(t, out.Growth, 1)
	assert.Equal(t, "orders", out.Growth[0].Topic)
	assert.NotEmpty(t, out.Growth[0].Sparkline)
}

func TestAnalyzeNoData(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze"})
	require.NoError(t, root.Execute())
	assert.Equal(t, "no data\n", buf.String())
}

func TestAnalyzeInvalidWindow(t *testing.T) {
	t.Setenv("STREAMPULSE_STORAGE_SQLITE_PATH", filepath.Join(t.TempDir(), "empty.db"))

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"analyze", "--window", "3x"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "window")
}
