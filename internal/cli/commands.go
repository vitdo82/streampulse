package cli

import (
	"github.com/pulsedev/streampulse/internal/tui"
	"github.com/spf13/cobra"
)

func runTUI() error {
	return tui.Run()
}

func newServeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start daemon mode — continuous metrics collection and alerting",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: start daemon
		},
	}
}

func newCheckCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "One-shot health check for CI/CD pipelines",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: run health check
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
			return nil // TODO: list DLQ topics
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "inspect",
		Short: "Inspect messages in a DLQ topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: inspect DLQ messages
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "replay",
		Short: "Replay DLQ messages to original topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: replay DLQ
		},
	})

	return cmd
}

func newAnalyzeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "analyze",
		Short: "Analytics — trends, growth, partition skew",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: analytics
		},
	}
}

func newAlertsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "alerts",
		Short: "View current alert status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // TODO: show alerts
		},
	}
}
