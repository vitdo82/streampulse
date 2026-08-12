// Package check implements the CI health checks behind the check command:
// connectivity, topic, consumer-group, retention, and replication gates that
// exit 0 when healthy and 1 or 2 when not.
package check

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pulsedev/streampulse/internal/kafka"
)

// Status is the outcome of a single check.
type Status string

const (
	StatusPass Status = "pass"
	StatusFail Status = "fail"
	StatusSkip Status = "skip"
)

// Result is the outcome of a single check.
type Result struct {
	Name    string
	Status  Status
	Message string
	Value   float64
}

// Check is a single health check. Run returns the check result, or an error
// which RunAll converts into a failing result (a monitoring gap is itself a
// health problem, never a skip).
type Check struct {
	Name string
	Run  func(ctx context.Context, env Env) (Result, error)
}

// Client is the subset of the kafka client the checks need.
type Client interface {
	Ping(ctx context.Context) error
	PartitionHealth(ctx context.Context, topic string) (partitions, errored int, err error)
	DescribeCluster(ctx context.Context) (*kafka.ClusterInfo, error)
	ListConsumerGroups(ctx context.Context) ([]kafka.GroupInfo, error)
	GroupLag(ctx context.Context) (map[string]map[string]int64, error)
}

// Compile-time check that the kafka client satisfies the check contract.
var _ Client = (*kafka.Client)(nil)

// Env carries the client and flags used by the checks.
type Env struct {
	Client Client
	Flags  Flags
}

// Flags holds the check options set from the command line.
type Flags struct {
	Topics            []string
	Groups            []string
	MinPartitions     int
	MaxLag            int64
	MinRetentionHours float64
	CheckReplication  bool
	Timeout           time.Duration
}

// Defaults applied when the corresponding flag is left at zero.
const (
	DefaultMinPartitions = 1
	DefaultMaxLag        = int64(1000)
)

// ConnectivityCheck is the name of the connectivity check result.
const ConnectivityCheck = "connectivity"

// RunAll runs every applicable check sequentially and returns all results,
// even after a failure. If the connectivity check fails nothing else can
// run, so the remaining checks are reported as skipped. A positive
// Flags.Timeout bounds the whole run.
func RunAll(ctx context.Context, env Env) []Result {
	if env.Flags.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, env.Flags.Timeout)
		defer cancel()
	}

	checks := []Check{{Name: ConnectivityCheck, Run: runConnectivity}}
	for _, topic := range env.Flags.Topics {
		checks = append(checks, checkTopic(topic))
	}

	results := make([]Result, 0, len(checks))
	connected := true
	for _, c := range checks {
		if !connected {
			results = append(results, Result{Name: c.Name, Status: StatusSkip, Message: "skipped: connectivity check failed"})
			continue
		}
		result, err := c.Run(ctx, env)
		if err != nil {
			result = Result{Name: c.Name, Status: StatusFail, Message: err.Error()}
		} else {
			result.Name = c.Name
		}
		if c.Name == ConnectivityCheck && result.Status == StatusFail {
			connected = false
		}
		results = append(results, result)
	}
	return results
}

// Verdict maps check results to a process exit code: 0 all checks passed,
// 1 at least one check failed, 2 the connectivity check failed (cluster
// unreachable or auth error — a pipeline problem, not a health verdict).
func Verdict(results []Result) int {
	for _, r := range results {
		if r.Name == ConnectivityCheck && r.Status == StatusFail {
			return 2
		}
	}
	for _, r := range results {
		if r.Status == StatusFail {
			return 1
		}
	}
	return 0
}

// runConnectivity verifies the cluster is reachable.
func runConnectivity(ctx context.Context, env Env) (Result, error) {
	if err := env.Client.Ping(ctx); err != nil {
		return Result{}, fmt.Errorf("ping: %w", err)
	}
	return Result{Status: StatusPass, Message: "brokers reachable", Value: 1}, nil
}

// checkTopic builds the health check for one topic: it must exist, have at
// least the minimum partition count, and no partition may be in error.
func checkTopic(topic string) Check {
	return Check{
		Name: "topic " + topic,
		Run: func(ctx context.Context, env Env) (Result, error) {
			partitions, errored, err := env.Client.PartitionHealth(ctx, topic)
			if err != nil {
				return Result{}, fmt.Errorf("partitions: %w", err)
			}
			min := env.Flags.MinPartitions
			if min < 1 {
				min = DefaultMinPartitions
			}

			var problems []string
			if partitions == 0 {
				problems = append(problems, "no partitions")
			}
			if partitions < min {
				problems = append(problems, fmt.Sprintf("%d partitions, min %d", partitions, min))
			}
			if errored > 0 {
				problems = append(problems, fmt.Sprintf("%d partitions errored", errored))
			}
			if len(problems) > 0 {
				return Result{Status: StatusFail, Message: strings.Join(problems, "; ")}, nil
			}
			return Result{
				Status:  StatusPass,
				Message: fmt.Sprintf("%d partitions, min %d", partitions, min),
				Value:   float64(partitions),
			}, nil
		},
	}
}
