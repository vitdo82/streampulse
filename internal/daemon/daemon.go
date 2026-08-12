// Package daemon implements `streampulse serve`: a 24/7 process that scrapes
// Kafka metrics, persists and rolls them up, and exposes a Prometheus
// endpoint, all sharing one lifecycle with graceful shutdown.
package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
)

// shutdownGrace bounds how long Shutdown waits for loops and the HTTP server
// (daemon.md lifecycle step 6).
const shutdownGrace = 10 * time.Second

// Scraper is the scraping engine the daemon drives once per interval.
type Scraper interface {
	ScrapeAndStore(ctx context.Context) error
}

// Daemon hosts the scrape and rollup loops and the Prometheus endpoint.
// Run blocks until the context is canceled or Shutdown is called.
type Daemon struct {
	cfg     *config.Config
	store   storage.MetricsStore
	client  *kafka.Client
	scraper Scraper

	mu     sync.Mutex // guards cancel
	cancel context.CancelFunc
	once   sync.Once
	wg     sync.WaitGroup
}

// New creates a Daemon around the store and kafka client. The client is
// borrowed: the caller owns it and is responsible for Close.
func New(cfg *config.Config, store storage.MetricsStore, client *kafka.Client) *Daemon {
	interval, _ := cfg.ParseScrapeInterval()
	return &Daemon{
		cfg:     cfg,
		store:   store,
		client:  client,
		scraper: scraper.New(cfg.ClusterID, client, store, interval),
	}
}

// Run blocks until ctx is canceled (or Shutdown is called), then waits for
// all loops within the shutdown grace period and returns nil.
func (d *Daemon) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	d.mu.Lock()
	d.cancel = cancel
	d.mu.Unlock()
	d.startLoops(runCtx)
	<-runCtx.Done()
	return d.Shutdown()
}

// Shutdown stops the loops and waits for them within the grace period. It is
// idempotent and safe to call from a second-signal handler.
func (d *Daemon) Shutdown() error {
	d.once.Do(func() {
		d.mu.Lock()
		cancel := d.cancel
		d.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		waitTimeout(&d.wg, shutdownGrace)
	})
	return nil
}

// startLoops launches the daemon's background loops. The scrape and rollup
// loops arrive with Task 4B; the alert loop is Phase 5.
func (d *Daemon) startLoops(ctx context.Context) {
	// TODO(4B): scrape and rollup loops.
	// TODO(5E): alert loop.
	_ = ctx
}

// waitTimeout waits for wg up to timeout, reporting whether it finished.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
