// Package kafka provides the Kafka client wrapper using kafka-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Client wraps the kafka-go connection for admin and consumer operations.
type Client struct {
	brokers []string
}

// NewClient creates a new Kafka client.
func NewClient(brokers []string) *Client {
	return &Client{brokers: brokers}
}

// Ping checks connectivity to the Kafka cluster.
func (c *Client) Ping(ctx context.Context) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Brokers()
	return err
}

// ClusterInfo returns basic cluster metadata.
type ClusterInfo struct {
	BrokerCount  int
	ControllerID int
	Brokers      []BrokerInfo
}

// BrokerInfo holds metadata for a single Kafka broker.
type BrokerInfo struct {
	Host              string
	Port              int
	ID                int
	Rack              string
	LeaderPartitions  int
	ReplicaPartitions int
}

// DescribeCluster returns cluster metadata including the broker list with
// partition leader and replica counts derived from Metadata.
func (c *Client) DescribeCluster(ctx context.Context) (*ClusterInfo, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("controller: %w", err)
	}

	brokers, err := conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("brokers: %w", err)
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	leaderCount := make(map[int]int)
	replicaCount := make(map[int]int)
	for _, p := range partitions {
		if p.Error != nil {
			continue
		}
		leaderCount[p.Leader.ID]++
		for _, r := range p.Replicas {
			replicaCount[r.ID]++
		}
	}

	brokerInfos := make([]BrokerInfo, len(brokers))
	for i, b := range brokers {
		brokerInfos[i] = BrokerInfo{
			Host:              b.Host,
			Port:              b.Port,
			ID:                b.ID,
			Rack:              b.Rack,
			LeaderPartitions:  leaderCount[b.ID],
			ReplicaPartitions: replicaCount[b.ID],
		}
	}

	return &ClusterInfo{
		BrokerCount:  len(brokers),
		ControllerID: controller.ID,
		Brokers:      brokerInfos,
	}, nil
}

// GroupInfo holds metadata for a single consumer group.
type GroupInfo struct {
	Name    string
	State   string
	Members int
}

// ListConsumerGroups returns all consumer groups in the cluster.
// Uses kafka-go's Client (ListGroups + DescribeGroups), which supports
// SASL/TLS via Dialer configuration and multi-broker failover.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]GroupInfo, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	client := &kafka.Client{Transport: &kafka.Transport{Dial: dialer.DialFunc}}

	var errs []error
	for _, b := range c.brokers {
		conn, err := dialer.DialContext(ctx, "tcp", b)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		conn.Close()

		groups, err := c.groupsFromBroker(ctx, client, kafka.TCP(b))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", b, err))
			continue
		}
		return groups, nil
	}
	return nil, fmt.Errorf("all brokers failed: %w", errors.Join(errs...))
}

// groupsFromBroker lists and describes consumer groups through one broker.
func (c *Client) groupsFromBroker(ctx context.Context, client *kafka.Client, addr net.Addr) ([]GroupInfo, error) {
	listResp, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{Addr: addr})
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	if listResp.Error != nil {
		return nil, fmt.Errorf("list groups: %w", listResp.Error)
	}
	if len(listResp.Groups) == 0 {
		return nil, nil
	}

	ids := make([]string, 0, len(listResp.Groups))
	for _, g := range listResp.Groups {
		ids = append(ids, g.GroupID)
	}

	descResp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{Addr: addr, GroupIDs: ids})
	if err != nil {
		return nil, fmt.Errorf("describe groups: %w", err)
	}

	groups := make([]GroupInfo, 0, len(descResp.Groups))
	for _, g := range descResp.Groups {
		if g.Error != nil {
			continue
		}
		groups = append(groups, GroupInfo{Name: g.GroupID, State: g.GroupState, Members: len(g.Members)})
	}
	return groups, nil
}

// TopicInfo holds metadata for a single Kafka topic.
type TopicInfo struct {
	Name       string
	Partitions int
}

// ListTopics returns all non-internal topics in the cluster.
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	return partitionsToTopics(partitions), nil
}

// dial opens a connection to the first available broker.
func (c *Client) dial(ctx context.Context) (*kafka.Conn, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	var errs []error
	for _, b := range c.brokers {
		conn, err := dialer.DialContext(ctx, "tcp", b)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", b, err))
	}
	return nil, fmt.Errorf("dial all brokers: %w", errors.Join(errs...))
}

// partitionsToTopics groups kafka-go partitions by topic, filtering internal topics.
func partitionsToTopics(partitions []kafka.Partition) []TopicInfo {
	topicPartitions := make(map[string]int)
	for _, p := range partitions {
		if isInternalTopic(p.Topic) || p.Error != nil {
			continue
		}
		topicPartitions[p.Topic]++
	}

	topics := make([]TopicInfo, 0, len(topicPartitions))
	for name, count := range topicPartitions {
		topics = append(topics, TopicInfo{Name: name, Partitions: count})
	}

	sort.Slice(topics, func(i, j int) bool {
		return strings.Compare(topics[i].Name, topics[j].Name) < 0
	})

	return topics
}

// isInternalTopic reports whether a topic is an internal Kafka topic.
func isInternalTopic(name string) bool {
	return strings.HasPrefix(name, "__")
}
