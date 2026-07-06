// Package kafka provides the Kafka client wrapper using kafka-go.
package kafka

import (
	"context"
	"fmt"

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
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	_, err = conn.Brokers()
	return err
}

// ClusterInfo returns basic cluster metadata.
type ClusterInfo struct {
	BrokerCount    int
	ControllerID   int
	ClusterID      string
}

// DescribeCluster returns cluster metadata.
func (c *Client) DescribeCluster(ctx context.Context) (*ClusterInfo, error) {
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, err
	}

	brokers, err := conn.Brokers()
	if err != nil {
		return nil, err
	}

	return &ClusterInfo{
		BrokerCount:  len(brokers),
		ControllerID: controller.ID,
	}, nil
}
