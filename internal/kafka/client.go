// Package kafka provides the Kafka client wrapper using kafka-go.
package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
// Uses raw TCP requests since kafka-go does not expose ListGroups or DescribeGroups.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]GroupInfo, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	groupIDs, err := listGroupsTCP(ctx, c.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}

	if len(groupIDs) == 0 {
		return nil, nil
	}

	return describeGroupsTCP(ctx, c.brokers[0], groupIDs)
}

// listGroupsTCP sends a raw Kafka ListGroups v1 request over TCP
// and returns the discovered consumer group IDs.
func listGroupsTCP(ctx context.Context, addr string) ([]string, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, err
	}

	clientID := "streampulse"
	bodySize := 2 + 2 + 4 + 2 + len(clientID)
	buf := make([]byte, 4+bodySize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodySize))
	binary.BigEndian.PutUint16(buf[4:6], 16)  // api_key: ListGroups
	binary.BigEndian.PutUint16(buf[6:8], 1)   // api_version: 1
	binary.BigEndian.PutUint32(buf[8:12], 42) // correlation_id
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)

	if _, err := conn.Write(buf); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, fmt.Errorf("read size: %w", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf)

	resp := make([]byte, respSize)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if len(resp) < 10 {
		return nil, fmt.Errorf("response too short: %d bytes", len(resp))
	}

	// v1 response: correlation_id (4) + throttle_time_ms (4) + error_code (2) + group_count (4)
	errCode := binary.BigEndian.Uint16(resp[8:10])
	if errCode != 0 {
		return nil, fmt.Errorf("list groups error code %d", errCode)
	}

	groupCount := int32(binary.BigEndian.Uint32(resp[10:14]))
	if groupCount <= 0 {
		return nil, nil
	}

	ids := make([]string, 0, groupCount)
	pos := 14
	for i := int32(0); i < groupCount; i++ {
		if pos+2 > len(resp) {
			break
		}
		nameLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		pos += 2
		if nameLen <= 0 || pos+int(nameLen) > len(resp) {
			break
		}
		ids = append(ids, string(resp[pos:pos+int(nameLen)]))
		pos += int(nameLen)

		if pos+2 > len(resp) {
			break
		}
		protoLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		pos += 2
		if protoLen > 0 {
			pos += int(protoLen)
		}
		if pos > len(resp) {
			break
		}
	}

	return ids, nil
}

// describeGroupsTCP sends a raw Kafka DescribeGroups v0 request and returns
// group details including state and member count.
func describeGroupsTCP(ctx context.Context, addr string, groupIDs []string) ([]GroupInfo, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, err
	}

	clientID := "streampulse"
	groupsSize := 0
	for _, g := range groupIDs {
		groupsSize += 2 + len(g)
	}
	bodySize := 2 + 2 + 4 + 2 + len(clientID) + 4 + groupsSize

	buf := make([]byte, 4+bodySize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(bodySize))
	binary.BigEndian.PutUint16(buf[4:6], 15) // api_key: DescribeGroups
	binary.BigEndian.PutUint16(buf[6:8], 0)  // api_version: 0
	binary.BigEndian.PutUint32(buf[8:12], 43)
	binary.BigEndian.PutUint16(buf[12:14], uint16(len(clientID)))
	copy(buf[14:], clientID)

	pos := 14 + len(clientID)
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(groupIDs)))
	pos += 4
	for _, g := range groupIDs {
		binary.BigEndian.PutUint16(buf[pos:pos+2], uint16(len(g)))
		pos += 2
		copy(buf[pos:], g)
		pos += len(g)
	}

	if _, err := conn.Write(buf); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, fmt.Errorf("read size: %w", err)
	}
	respSize := binary.BigEndian.Uint32(sizeBuf)

	resp := make([]byte, respSize)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	if len(resp) < 8 {
		return nil, fmt.Errorf("response too short: %d bytes", len(resp))
	}

	// v0 response layout: correlation_id (4) + group_count (4) — no throttle_time_ms
	groupCount := int32(binary.BigEndian.Uint32(resp[4:8]))
	if groupCount <= 0 {
		return nil, nil
	}

	groups := make([]GroupInfo, 0, groupCount)
	pos = 8
	for i := int32(0); i < groupCount; i++ {
		if pos+2 > len(resp) {
			break
		}
		errCode := binary.BigEndian.Uint16(resp[pos : pos+2])
		pos += 2

		if pos+2 > len(resp) {
			break
		}
		idLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
		pos += 2
		if idLen <= 0 || pos+int(idLen) > len(resp) {
			break
		}
		name := string(resp[pos : pos+int(idLen)])
		pos += int(idLen)

		state := "Unknown"
		if pos+2 <= len(resp) {
			stateLen := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
			pos += 2
			if stateLen > 0 && pos+int(stateLen) <= len(resp) {
				state = string(resp[pos : pos+int(stateLen)])
				pos += int(stateLen)
			}
		}

		skipString := func() {
			if pos+2 > len(resp) {
				return
			}
			l := int16(binary.BigEndian.Uint16(resp[pos : pos+2]))
			pos += 2
			if l > 0 {
				pos += int(l)
			}
		}
		skipString() // protocol_type
		skipString() // protocol

		members := 0
		if pos+4 <= len(resp) {
			memberCount := int32(binary.BigEndian.Uint32(resp[pos : pos+4]))
			pos += 4
			members = int(memberCount)
			for m := int32(0); m < memberCount && pos < len(resp); m++ {
				skipString() // member_id
				skipString() // client_id
				skipString() // client_host
				if pos+4 > len(resp) {
					break
				}
				metaLen := int32(binary.BigEndian.Uint32(resp[pos : pos+4]))
				pos += 4 + int(metaLen)
				if pos+4 > len(resp) {
					break
				}
				assignLen := int32(binary.BigEndian.Uint32(resp[pos : pos+4]))
				pos += 4 + int(assignLen)
			}
		}

		if errCode != 0 {
			continue
		}
		groups = append(groups, GroupInfo{Name: name, State: state, Members: members})
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

	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	return conn, nil
}

// partitionsToTopics groups kafka-go partitions by topic, filtering internal topics.
func partitionsToTopics(partitions []kafka.Partition) []TopicInfo {
	topicPartitions := make(map[string]int)
	for _, p := range partitions {
		if isInternalTopic(p.Topic) {
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
