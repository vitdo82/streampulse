package cli

import (
	"os"
	"os/signal"
	"syscall"

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
		client = kafka.NewClient(cfg.Brokers)
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

			d := daemon.New(cfg, store, client)

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
	return &cobra.Command{
		Use:   "alerts",
		Short: "View current alert status",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := CfgFromContext(cmd.Context())
			if err != nil {
				return err
			}
			_ = cfg // TODO: show alerts (Phase 5)
			return nil
		},
	}
}
