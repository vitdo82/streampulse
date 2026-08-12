// Package scraper collects Kafka cluster metrics and persists them through the
// storage backend.
package scraper

import (
	"context"
	"errors"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
)

const (
	// scrapeTimeout bounds one full scrape cycle.
	scrapeTimeout = 5 * time.Second
)

// Client is the subset of the kafka client the collectors depend on.
type Client interface {
	DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error)
	ListTopics(ctx context.Context) ([]kafka.TopicInfo, error)
	ListConsumerGroups(ctx context.Context) ([]kafka.GroupInfo, error)
}

// Compile-time check that the concrete kafka client satisfies the interface.
var _ Client = (*kafka.Client)(nil)

// Collector scrapes one metric family into storage.Metric rows.
type Collector interface {
	Collect(ctx context.Context, now time.Time) ([]storage.Metric, error)
}

// Scraper runs the collectors sequentially within one timeout context and
// persists the resulting metrics in a single batch per cycle.
type Scraper struct {
	collectors []Collector
	store      storage.MetricsStore
	clusterID  string
}

// NewWithCollectors creates a Scraper with a custom collector set.
func NewWithCollectors(clusterID string, store storage.MetricsStore, collectors []Collector) *Scraper {
	return &Scraper{clusterID: clusterID, store: store, collectors: collectors}
}

// Collect runs every collector sequentially and returns their metrics. A
// failing collector does not prevent the others from reporting; its error is
// returned joined with the rest.
func (s *Scraper) Collect(ctx context.Context) ([]storage.Metric, error) {
	scrapeCtx, cancel := context.WithTimeout(ctx, scrapeTimeout)
	defer cancel()
	now := time.Now()

	var all []storage.Metric
	var errs []error
	for _, c := range s.collectors {
		metrics, err := c.Collect(scrapeCtx, now)
		if err != nil {
			errs = append(errs, err)
		}
		all = append(all, metrics...)
	}
	for i := range all {
		all[i].ClusterID = s.clusterID
	}
	return all, errors.Join(errs...)
}
