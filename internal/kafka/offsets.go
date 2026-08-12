package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// TopicOffsets returns the high-watermark offset of every partition of every
// non-internal topic, keyed by topic then partition. Partitions whose metadata
// or offset lookup failed are skipped.
func (c *Client) TopicOffsets(ctx context.Context) (map[string]map[int]int64, error) {
	if len(c.brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}

	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	req := &kafka.ListOffsetsRequest{Addr: conn.RemoteAddr(), Topics: make(map[string][]kafka.OffsetRequest)}
	offsets := make(map[string]map[int]int64)
	for _, p := range partitions {
		if isInternalTopic(p.Topic) || p.Error != nil {
			continue
		}
		if _, ok := offsets[p.Topic]; !ok {
			offsets[p.Topic] = make(map[int]int64)
		}
		req.Topics[p.Topic] = append(req.Topics[p.Topic], kafka.LastOffsetOf(p.ID))
	}
	if len(req.Topics) == 0 {
		return offsets, nil
	}

	resp, err := c.adminClient.ListOffsets(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	for topic, parts := range resp.Topics {
		m, ok := offsets[topic]
		if !ok {
			continue
		}
		for _, p := range parts {
			if p.Error != nil || p.LastOffset < 0 {
				continue
			}
			m[p.Partition] = p.LastOffset
		}
	}
	return offsets, nil
}
