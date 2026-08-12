package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// brokerAvailable reports whether a local test broker (docker compose,
// localhost:9093) answers a TCP dial.
func brokerAvailable() bool {
	conn, err := net.DialTimeout("tcp", "localhost:9093", time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func TestDLQInspectRequiresTopic(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "inspect"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--topic")
}

func TestDLQReplayRequiresTopic(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "replay", "--dry-run"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--topic")
}

func TestDLQReplayRejectsBadFilter(t *testing.T) {
	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "replay", "--topic", "x.dlq", "--filter", "novalue"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key=value")
}

// TestDLQListJSONIntegration lists DLQ topics against the docker compose
// cluster (skipped when the broker is down) and asserts the convention-based
// discovery finds payments.dlq with its original topic resolved.
func TestDLQListJSONIntegration(t *testing.T) {
	if !brokerAvailable() {
		t.Skip("no local Kafka broker (docker compose) available")
	}

	var buf bytes.Buffer
	root := NewRootCommand("test")
	root.SetOut(&buf)
	root.SetArgs([]string{"dlq", "list", "--json"})
	require.NoError(t, root.Execute())

	var rows []struct {
		Name           string `json:"name"`
		OriginalTopic  string `json:"original_topic"`
		OriginalExists bool   `json:"original_exists"`
		MessageCount   int64  `json:"message_count"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rows))
	require.NotEmpty(t, rows, "expected at least one DLQ topic")

	found := false
	for _, r := range rows {
		if r.Name == "payments.dlq" {
			found = true
			assert.Equal(t, "payments", r.OriginalTopic)
			assert.True(t, r.OriginalExists)
			break
		}
	}
	assert.True(t, found, fmt.Sprintf("payments.dlq missing from %+v", rows))
}
