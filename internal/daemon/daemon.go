// Package daemon implements `streampulse serve`: a 24/7 process that scrapes
// Kafka metrics, persists and rolls them up, and exposes a Prometheus
// endpoint, all sharing one lifecycle with graceful shutdown.
package daemon

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/scraper"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/robfig/cron/v3"
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

	scraping atomic.Bool // in-flight guard for the scrape loop

	mu      sync.Mutex // guards cancel
	cancel  context.CancelFunc
	stopped chan struct{} // closed by Run once all loops have exited
	once    sync.Once
	wg      sync.WaitGroup
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
		stopped: make(chan struct{}),
	}
}

// Run blocks until ctx is canceled (or Shutdown is called), then waits for
// all loops to exit and returns nil.
func (d *Daemon) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	d.mu.Lock()
	d.cancel = cancel
	d.mu.Unlock()

	d.startLoops(runCtx) // registers all loop goroutines synchronously
	d.wg.Wait()          // loops exit when runCtx is canceled
	close(d.stopped)
	return nil
}

// Shutdown cancels the loop context and waits for the loops within the
// shutdown grace period. It is idempotent and safe to call from a
// second-signal handler.
func (d *Daemon) Shutdown() error {
	d.once.Do(func() {
		d.mu.Lock()
		cancel := d.cancel
		d.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		select {
		case <-d.stopped:
		case <-time.After(shutdownGrace):
		}
	})
	return nil
}

// goLoop launches a daemon loop, registering it with the WaitGroup
// synchronously so Shutdown's wait can never race loop startup.
func (d *Daemon) goLoop(fn func()) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		fn()
	}()
}

// startLoops launches the daemon's background loops.
func (d *Daemon) startLoops(ctx context.Context) {
	interval, err := d.cfg.ParseScrapeInterval()
	if err != nil {
		slog.Error("invalid scrape interval, using 5s", "err", err)
		interval = 5 * time.Second
	}

	d.goLoop(func() { runScrapeLoop(ctx, d, time.NewTicker(interval).C) })
	d.goLoop(func() { runRollupLoop(ctx, d, cron.New()) })

	// TODO(5E): alert loop.
}
