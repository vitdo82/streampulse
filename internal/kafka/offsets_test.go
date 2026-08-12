package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicOffsetsNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.TopicOffsets(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no brokers")
}

func TestTopicOffsetsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	offsets, err := client.TopicOffsets(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	require.Contains(t, offsets, "orders", "orders topic expected in offsets")
	require.Len(t, offsets["orders"], 6, "orders has 6 partitions")
	for partition, offset := range offsets["orders"] {
		require.GreaterOrEqual(t, offset, int64(0), "partition %d", partition)
	}
	for name := range offsets {
		assert.False(t, isInternalTopic(name), "internal topic %q must not be collected", name)
	}
}
