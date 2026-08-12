package scraper

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
)

// brokerCollector emits per-broker leader and replica partition counts from
// DescribeCluster.
type brokerCollector struct {
	client    Client
	clusterID string
}

func newBrokerCollector(client Client, clusterID string) *brokerCollector {
	return &brokerCollector{client: client, clusterID: clusterID}
}

func (b *brokerCollector) Collect(ctx context.Context, now time.Time) ([]storage.Metric, error) {
	info, err := b.client.DescribeCluster(ctx)
	if err != nil {
		return nil, fmt.Errorf("describe cluster: %w", err)
	}
	metrics := make([]storage.Metric, 0, len(info.Brokers)*2)
	for _, br := range info.Brokers {
		metrics = append(metrics,
			storage.Metric{TS: now, ClusterID: b.clusterID, Metric: MetricBrokerLeaderPartitions, EntityType: "broker", EntityName: brokerName(br), Value: float64(br.LeaderPartitions)},
			storage.Metric{TS: now, ClusterID: b.clusterID, Metric: MetricBrokerReplicaPartitions, EntityType: "broker", EntityName: brokerName(br), Value: float64(br.ReplicaPartitions)},
		)
	}
	return metrics, nil
}

// brokerName formats a broker as host:port, falling back to the broker ID when
// the host is unknown (offline broker).
func brokerName(b kafka.BrokerInfo) string {
	if b.Host == "" {
		return strconv.Itoa(b.ID)
	}
	return net.JoinHostPort(b.Host, strconv.Itoa(b.Port))
}
