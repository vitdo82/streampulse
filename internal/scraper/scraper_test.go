package scraper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testClusterID = "local-dev"

// fakeClient satisfies the scraper Client interface. The interface grows per
// task: 2A adds the existing kafka methods, 2B TopicOffsets, 2C GroupLag.
type fakeClient struct {
	cluster *kafka.ClusterInfo
	topics  []kafka.TopicInfo
	groups  []kafka.GroupInfo
	offsets map[string]map[int]int64
	lags    map[string]map[string]int64
	err     error
}

func (f *fakeClient) DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error) {
	return f.cluster, f.err
}

func (f *fakeClient) ListTopics(ctx context.Context) ([]kafka.TopicInfo, error) {
	return f.topics, f.err
}

func (f *fakeClient) ListConsumerGroups(ctx context.Context) ([]kafka.GroupInfo, error) {
	return f.groups, f.err
}

type fakeCollector struct {
	metrics []storage.Metric
	err     error
}

func (f *fakeCollector) Collect(ctx context.Context, now time.Time) ([]storage.Metric, error) {
	return f.metrics, f.err
}

// fakeStore implements storage.MetricsStore and records WriteBatch calls.
type fakeStore struct {
	batches [][]storage.Metric
	err     error
}

func (f *fakeStore) WriteBatch(ctx context.Context, metrics []storage.Metric) error {
	if f.err != nil {
		return f.err
	}
	f.batches = append(f.batches, metrics)
	return nil
}

func (f *fakeStore) QueryRaw(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}

func (f *fakeStore) QueryHourly(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}

func (f *fakeStore) QueryDaily(ctx context.Context, params storage.QueryParams) ([]storage.MetricRow, error) {
	return nil, nil
}

func (f *fakeStore) Rollup(ctx context.Context, resolution string) error {
	return nil
}

func (f *fakeStore) Purge(ctx context.Context, retention storage.Retention) error {
	return nil
}

func (f *fakeStore) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeStore) Migrate(ctx context.Context) error {
	return nil
}

func (f *fakeStore) Close() error {
	return nil
}

func TestMetricNames(t *testing.T) {
	names := map[string]string{
		"kafka.broker.leader_partitions":  MetricBrokerLeaderPartitions,
		"kafka.broker.replica_partitions": MetricBrokerReplicaPartitions,
		"kafka.topic.partition_count":     MetricTopicPartitionCount,
		"kafka.topic.messages":            MetricTopicMessages,
		"kafka.topic.msg_rate":            MetricTopicMsgRate,
		"kafka.topic.bytes_rate":          MetricTopicBytesRate,
		"kafka.group.lag":                 MetricGroupLag,
		"kafka.group.member_count":        MetricGroupMemberCount,
		"kafka.group.state":               MetricGroupState,
	}
	for want, got := range names {
		assert.Equal(t, want, got, "metric constant %q", want)
	}
}

func TestBrokerCollector(t *testing.T) {
	now := time.Now()
	client := &fakeClient{cluster: &kafka.ClusterInfo{
		ControllerID: 1,
		Brokers: []kafka.BrokerInfo{
			{Host: "broker-1", Port: 9092, ID: 1, LeaderPartitions: 4, ReplicaPartitions: 6},
			{Host: "broker-2", Port: 9093, ID: 2, LeaderPartitions: 2, ReplicaPartitions: 3},
		},
	}}
	c := newBrokerCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), now)
	require.NoError(t, err)
	require.Len(t, metrics, 4)

	got := make(map[string]map[string]float64)
	for _, m := range metrics {
		assert.Equal(t, now, m.TS)
		assert.Equal(t, testClusterID, m.ClusterID)
		assert.Equal(t, "broker", m.EntityType)
		if got[m.EntityName] == nil {
			got[m.EntityName] = make(map[string]float64)
		}
		got[m.EntityName][m.Metric] = m.Value
	}
	assert.Equal(t, 4.0, got["broker-1:9092"][MetricBrokerLeaderPartitions])
	assert.Equal(t, 6.0, got["broker-1:9092"][MetricBrokerReplicaPartitions])
	assert.Equal(t, 2.0, got["broker-2:9093"][MetricBrokerLeaderPartitions])
	assert.Equal(t, 3.0, got["broker-2:9093"][MetricBrokerReplicaPartitions])
}

func TestBrokerCollectorOfflineBrokerName(t *testing.T) {
	client := &fakeClient{cluster: &kafka.ClusterInfo{Brokers: []kafka.BrokerInfo{{ID: 3, LeaderPartitions: 1, ReplicaPartitions: 1}}}}
	c := newBrokerCollector(client, testClusterID)

	metrics, err := c.Collect(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, metrics, 2)
	assert.Equal(t, "3", metrics[0].EntityName)
}

func TestBrokerCollectorError(t *testing.T) {
	client := &fakeClient{err: errors.New("cluster down")}
	c := newBrokerCollector(client, testClusterID)

	_, err := c.Collect(context.Background(), time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster down")
}

func TestScraperCollect(t *testing.T) {
	store := &fakeStore{}
	s := NewWithCollectors(testClusterID, store, []Collector{
		&fakeCollector{metrics: []storage.Metric{{Metric: "m1", Value: 1}}},
		&fakeCollector{metrics: []storage.Metric{{Metric: "m2", Value: 2}}},
	})

	metrics, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, metrics, 2)
	for _, m := range metrics {
		assert.Equal(t, testClusterID, m.ClusterID)
	}
}
