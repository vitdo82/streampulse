// Package cli provides the cobra command tree for StreamPulse.
package cli

import (
	"github.com/spf13/cobra"
)

// NewRootCommand creates the root streampulse command with all subcommands.
func NewRootCommand(version string) *cobra.Command {
	var brokers []string

	root := &cobra.Command{
		Use:   "streampulse",
		Short: "StreamPulse — the k9s for Apache Kafka",
		Long: `StreamPulse is a real-time Kafka observability tool with a k9s-style terminal UI.

Single binary. Zero dependencies.

  streampulse                              # interactive TUI (mock data)
  streampulse --brokers localhost:9092     # interactive TUI connected to Kafka
  streampulse serve --brokers kafka:9092   # daemon mode (24/7 monitoring)
  streampulse check --topic orders         # CI/CD health gate
  streampulse dlq list                     # DLQ management`,
		Version: version,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTUI(brokers)
		},
	}

	root.PersistentFlags().StringSliceVar(&brokers, "brokers", nil, "Kafka broker addresses (comma-separated)")

	root.AddCommand(newServeCommand())
	root.AddCommand(newCheckCommand())
	root.AddCommand(newDLQCommand())
	root.AddCommand(newAnalyzeCommand())
	root.AddCommand(newAlertsCommand())

	return root
}
