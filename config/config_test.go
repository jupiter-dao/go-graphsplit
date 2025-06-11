package config

import (
	"os"
	"testing"

	"github.com/gozelle/testify/require"
)

func TestConfig(t *testing.T) {
	cfg := NewConfig()

	data, err := generateTOMLWithComments(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile("example.toml", []byte(data), 0644))

	loadedCfg, err := LoadConfig("example.toml")
	require.NoError(t, err)

	if loadedCfg.SliceSize != cfg.SliceSize {
		t.Errorf("expected loaded slice size to be %d, got %d", cfg.SliceSize, loadedCfg.SliceSize)
	}
}
