package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/robfig/cron/v3"
)

// runScrapeLoop scrapes once per tick, dropping a tick when the previous
// scrape is still in flight (deadline guard, mirroring the TUI loading
// pattern). The tick channel is injected so tests control the cadence.
func runScrapeLoop(ctx context.Context, d *Daemon, tick <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			d.scrapeOnce(ctx)
		}
	}
}

// scrapeOnce runs one scrape cycle asynchronously. A tick arriving while a
// scrape is still in flight is skipped: only one scrape may run at a time.
func (d *Daemon) scrapeOnce(ctx context.Context) {
	if !d.scraping.CompareAndSwap(false, true) {
		return // previous scrape still running; skip this tick
	}
	go func() {
		defer d.scraping.Store(false)
		d.doScrape(ctx)
	}()
}

// doScrape performs one scrape cycle and records its outcome.
func (d *Daemon) doScrape(ctx context.Context) {
	if err := d.scraper.ScrapeAndStore(ctx); err != nil {
		slog.Error("scrape failed", "err", err)
	}
}

// runRollupLoop schedules the hourly and daily rollups via cron and runs
// until the context is canceled. The cron instance is injected so tests can
// drive the cadence.
func runRollupLoop(ctx context.Context, d *Daemon, c *cron.Cron) {

	if err := scheduleRollup(c, d, ctx); err != nil {
		slog.Error("rollup schedule", "err", err)
		return
	}
	c.Start()
	defer c.Stop()
	<-ctx.Done()
}

// scheduleRollup registers the hourly (at minute 5) and daily (at 00:10)
// rollup jobs per daemon.md.
func scheduleRollup(c *cron.Cron, d *Daemon, ctx context.Context) error {
	if _, err := c.AddFunc("5 * * * *", func() { d.rollupOnce(ctx, "hourly") }); err != nil {
		return fmt.Errorf("hourly rollup: %w", err)
	}
	if _, err := c.AddFunc("10 0 * * *", func() { d.rollupOnce(ctx, "daily") }); err != nil {
		return fmt.Errorf("daily rollup: %w", err)
	}
	return nil
}

// rollupOnce aggregates one resolution and then purges expired data
// (daemon.md: Purge runs after rollup).
func (d *Daemon) rollupOnce(ctx context.Context, resolution string) {
	if err := d.store.Rollup(ctx, resolution); err != nil {
		slog.Error("rollup failed", "resolution", resolution, "err", err)
		return
	}
	if err := d.store.Purge(ctx, d.retention()); err != nil {
		slog.Error("purge failed", "err", err)
	}
}

// retention returns the per-resolution retention defaults from storage.md:
// raw 24h, hourly 90d, daily 365d.
func (d *Daemon) retention() storage.Retention {
	return storage.Retention{
		Raw:    24 * time.Hour,
		Hourly: 90 * 24 * time.Hour,
		Daily:  365 * 24 * time.Hour,
	}
}
