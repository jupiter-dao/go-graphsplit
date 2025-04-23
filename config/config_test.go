package config

import (
	"testing"

	"github.com/gozelle/testify/require"
)

func TestConfig(t *testing.T) {
	cfg := NewConfig()

	err := cfg.SaveConfig("example.toml")
	require.NoError(t, err)

	loadedCfg, err := LoadConfig("example.toml")
	require.NoError(t, err)

	if loadedCfg.SliceSize != cfg.SliceSize {
		t.Errorf("expected loaded slice size to be %d, got %d", cfg.SliceSize, loadedCfg.SliceSize)
	}
}
