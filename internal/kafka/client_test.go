package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionsToTopics(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "orders", ID: 0},
		{Topic: "orders", ID: 1},
		{Topic: "orders", ID: 2},
		{Topic: "payments", ID: 0},
		{Topic: "__consumer_offsets", ID: 0},
		{Topic: "audit", ID: 0},
	}

	topics := partitionsToTopics(partitions)

	assert.Len(t, topics, 3)
	assert.Equal(t, "audit", topics[0].Name)
	assert.Equal(t, 1, topics[0].Partitions)
	assert.Equal(t, "orders", topics[1].Name)
	assert.Equal(t, 3, topics[1].Partitions)
	assert.Equal(t, "payments", topics[2].Name)
	assert.Equal(t, 1, topics[2].Partitions)
}

func TestPartitionsToTopicsEmpty(t *testing.T) {
	topics := partitionsToTopics(nil)
	assert.Empty(t, topics)
}

func TestIsInternalTopic(t *testing.T) {
	assert.True(t, isInternalTopic("__consumer_offsets"))
	assert.True(t, isInternalTopic("__transaction_state"))
	assert.False(t, isInternalTopic("orders"))
	assert.False(t, isInternalTopic("_schemas"))
}

func TestNewClient(t *testing.T) {
	c := NewClient([]string{"localhost:9092"})
	assert.NotNil(t, c)
}

func TestListTopicsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topics, err := client.ListTopics(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	assert.NotNil(t, topics)
	t.Logf("discovered topics: %+v", topics)
}

func TestListConsumerGroupsIntegration(t *testing.T) {
	broker := os.Getenv("STREAMPULSE_TEST_BROKER")
	if broker == "" {
		broker = "localhost:9093"
	}

	client := NewClient([]string{broker})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	groups, err := client.ListConsumerGroups(ctx)
	if err != nil {
		t.Skipf("Kafka not available at %s: %v", broker, err)
	}

	t.Logf("discovered groups: %+v", groups)
}

func TestDialFailoverTriesAllBrokers(t *testing.T) {
	c := NewClient([]string{"127.0.0.1:1", "127.0.0.1:2"})
	err := c.Ping(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "127.0.0.1:1")
	assert.Contains(t, err.Error(), "127.0.0.1:2")
}

func TestListConsumerGroupsNoBrokers(t *testing.T) {
	c := NewClient(nil)
	_, err := c.ListConsumerGroups(context.Background())
	require.Error(t, err)
}

func TestPartitionsToTopicsSkipsErroredPartitions(t *testing.T) {
	partitions := []kafka.Partition{
		{Topic: "orders", ID: 0, Error: fmt.Errorf("leader not available")},
		{Topic: "orders", ID: 1},
		{Topic: "__consumer_offsets", ID: 0},
	}
	topics := partitionsToTopics(partitions)
	require.Len(t, topics, 1)
	assert.Equal(t, "orders", topics[0].Name)
	assert.Equal(t, 1, topics[0].Partitions)
}

func TestTransportGoroutinesReleasedOnClose(t *testing.T) {
	// Dummy broker: accepts TCP connections so the transport creates its pool
	// and metadata-discover goroutine (an unreachable address never reaches
	// RoundTrip and would make this test vacuous).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	before := runtime.NumGoroutine()
	for i := 0; i < 2; i++ {
		c := NewClient([]string{l.Addr().String()})
		_, _ = c.ListConsumerGroups(context.Background())
		c.Close()
	}
	// Allow background goroutines to unwind.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if runtime.NumGoroutine() > before+1 {
		t.Fatalf("goroutines not released after Close: before=%d after=%d", before, runtime.NumGoroutine())
	}
}
