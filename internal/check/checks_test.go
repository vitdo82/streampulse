package check

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsedev/streampulse/internal/kafka"
)

// fakeClient implements the check Client contract with configurable results.
type fakeClient struct {
	pingErr error

	partitions int
	errored    int
	partErr    error

	cluster   *kafka.ClusterInfo
	groups    []kafka.GroupInfo
	groupsErr error
	lag       map[string]map[string]int64
	lagErr    error
}

func (f *fakeClient) Ping(context.Context) error { return f.pingErr }

func (f *fakeClient) PartitionHealth(context.Context, string) (int, int, error) {
	return f.partitions, f.errored, f.partErr
}

func (f *fakeClient) DescribeCluster(context.Context) (*kafka.ClusterInfo, error) {
	return f.cluster, nil
}

func (f *fakeClient) ListConsumerGroups(context.Context) ([]kafka.GroupInfo, error) {
	return f.groups, f.groupsErr
}

func (f *fakeClient) GroupLag(context.Context) (map[string]map[string]int64, error) {
	return f.lag, f.lagErr
}

var _ Client = (*fakeClient)(nil)

func TestConnectivityOnly(t *testing.T) {
	env := Env{Client: &fakeClient{}, Flags: Flags{}}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 1)
	assert.Equal(t, "connectivity", results[0].Name)
	assert.Equal(t, StatusPass, results[0].Status)
	assert.Equal(t, 0, Verdict(results))
}

func TestConnectivityFailSkipsRemaining(t *testing.T) {
	env := Env{
		Client: &fakeClient{pingErr: errors.New("dial tcp 127.0.0.1:9093: connection refused")},
		Flags:  Flags{Topics: []string{"orders"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[0].Status, "connectivity must fail")
	assert.Equal(t, StatusSkip, results[1].Status, "checks after connectivity failure are skipped")
	assert.Equal(t, 2, Verdict(results))
}

func TestTopicCheckPass(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 6},
		Flags:  Flags{Topics: []string{"orders"}, MinPartitions: 6},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusPass, results[0].Status)
	assert.Equal(t, "topic orders", results[1].Name)
	assert.Equal(t, StatusPass, results[1].Status)
	assert.Equal(t, float64(6), results[1].Value)
	assert.Contains(t, results[1].Message, "6 partitions")
	assert.Equal(t, 0, Verdict(results))
}

func TestTopicCheckBelowMinPartitions(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 2},
		Flags:  Flags{Topics: []string{"orders"}, MinPartitions: 6},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "min 6")
	assert.Equal(t, 1, Verdict(results))
}

func TestTopicCheckErroredPartitions(t *testing.T) {
	env := Env{
		Client: &fakeClient{partitions: 6, errored: 2},
		Flags:  Flags{Topics: []string{"orders"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "errored")
	assert.Equal(t, 1, Verdict(results))
}

func TestTopicCheckMissingTopic(t *testing.T) {
	env := Env{
		Client: &fakeClient{partErr: errors.New("unknown topic or partition")},
		Flags:  Flags{Topics: []string{"nope"}},
	}
	results := RunAll(context.Background(), env)

	require.Len(t, results, 2)
	assert.Equal(t, StatusFail, results[1].Status)
	assert.Contains(t, results[1].Message, "unknown topic or partition")
}

func TestVerdict(t *testing.T) {
	pass := Result{Name: "connectivity", Status: StatusPass}
	fail := Result{Name: "topic orders", Status: StatusFail}
	skip := Result{Name: "topic orders", Status: StatusSkip}
	connFail := Result{Name: "connectivity", Status: StatusFail}

	cases := []struct {
		name    string
		results []Result
		want    int
	}{
		{"all pass", []Result{pass, pass}, 0},
		{"one fail among passes", []Result{pass, fail}, 1},
		{"connectivity fail", []Result{connFail, skip}, 2},
		{"empty results", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Verdict(tc.results))
		})
	}
}
