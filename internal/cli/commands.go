package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/pulsedev/streampulse/internal/config"
	"github.com/pulsedev/streampulse/internal/daemon"
	"github.com/pulsedev/streampulse/internal/kafka"
	"github.com/pulsedev/streampulse/internal/storage"
	"github.com/pulsedev/streampulse/internal/tui"
	"github.com/spf13/cobra"
)

func runTUI(cfg *config.Config) error {
	var client *kafka.Client
	if len(cfg.Brokers) > 0 {
		c, err := kafka.NewClientWithOptions(cfg.Brokers, kafka.Options{
			TLS: kafka.TLSOptions{
				Enabled:            cfg.Kafka.TLS.Enabled,
				CAFile:             cfg.Kafka.TLS.CAFile,
				CertFile:           cfg.Kafka.TLS.CertFile,
				KeyFile:            cfg.Kafka.TLS.KeyFile,
				InsecureSkipVerify: cfg.Kafka.TLS.InsecureSkipVerify,
			},
			SASL: kafka.SASLOptions{
				Mechanism:   cfg.Kafka.SASL.Mechanism,
				Username:    cfg.Kafka.SASL.Username,
				PasswordEnv: cfg.Kafka.SASL.PasswordEnv,
			},
		})
		if err != nil {
			return err
		}
		client = c
		defer client.Close()
	}
	return tui.Run(client)
}

func newServeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start daemon mode — continuous metrics collection and alerting",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}

			store, err := storage.NewStore(cfg.Storage.Type, cfg.Storage.SQLite.Path)
			if err != nil {
				return err
			}
			defer store.Close()

			client, err := kafka.NewClientWithOptions(cfg.Brokers, kafka.Options{
				TLS: kafka.TLSOptions{
					Enabled:            cfg.Kafka.TLS.Enabled,
					CAFile:             cfg.Kafka.TLS.CAFile,
					CertFile:           cfg.Kafka.TLS.CertFile,
					KeyFile:            cfg.Kafka.TLS.KeyFile,
					InsecureSkipVerify: cfg.Kafka.TLS.InsecureSkipVerify,
				},
				SASL: kafka.SASLOptions{
					Mechanism:   cfg.Kafka.SASL.Mechanism,
					Username:    cfg.Kafka.SASL.Username,
					PasswordEnv: cfg.Kafka.SASL.PasswordEnv,
				},
			})
			if err != nil {
				return err
			}
			defer client.Close()

			d := daemon.NewWithOptions(cfg, store, client, daemon.Options{Version: cmd.Root().Version})

			// SIGTERM/SIGINT cancel the daemon context; a second signal
			// takes the immediate-exit path (signal.NotifyContext stops
			// catching, so the default handler kills the process).
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return d.Run(ctx)
		},
	}
}

func newCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "One-shot health check for CI/CD pipelines",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			_ = cfg // TODO: run health check (Phase 8)
			return nil
		},
	}
}

func newDLQCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Dead letter queue management",
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "Auto-discover DLQ topics",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: list DLQ topics (Phase 6)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "inspect",
		Short: "Inspect messages in a DLQ topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: inspect DLQ messages (Phase 6)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "replay",
		Short: "Replay DLQ messages to original topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: replay DLQ (Phase 6)
		},
	})

	return cmd
}

func newAnalyzeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "analyze",
		Short: "Analytics — trends, growth, partition skew",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			_ = cfg // TODO: analytics (Phase 7)
			return nil
		},
	}
}

func newAlertsCommand() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "alerts",
		Short: "View current alert status",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}

			store, err := storage.NewStore(cfg.Storage.Type, cfg.Storage.SQLite.Path)
			if err != nil {
				return err
			}
			defer store.Close()

			rows, err := store.QueryAlertState(cmd.Context())
			if err != nil {
				return fmt.Errorf("query alert state: %w", err)
			}
			if len(rows) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no alerts")
				return nil
			}
			if jsonOut {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(rows)
			}
			printAlertTable(cmd.OutOrStdout(), rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Output alert state as JSON")
	return cmd
}

// printAlertTable renders the persisted alert states as an aligned table
// (rule, status, last fired, last value, notify count).
func printAlertTable(w io.Writer, rows []storage.AlertStateRow) {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "RULE\tSTATUS\tLAST FIRED\tVALUE\tNOTIFY COUNT")
	for _, r := range rows {
		lastFired := "-"
		if !r.LastFired.IsZero() {
			lastFired = r.LastFired.Format(time.RFC3339)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.2f\t%d\n", r.RuleName, r.Status, lastFired, r.LastValue, r.NotifyCount)
	}
	tw.Flush()
}
