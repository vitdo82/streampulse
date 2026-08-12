package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRootLoadsConfig(t *testing.T) {
	root := NewRootCommand("test")
	root.SetArgs([]string{"--config", "/nonexistent", "serve"})
	err := root.Execute()
	require.Error(t, err) // nonexistent file → error before serve runs
}
