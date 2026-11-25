package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseArgs_DefaultsAndEnvPrecedence(t *testing.T) {
	t.Setenv("ARGO_SERVER", "http://argo-server.argo.svc.cluster.local:2746")
	t.Setenv("MCP_AUTH_TOKEN", "env-inbound-secret")
	t.Setenv("MCP_TOOLSETS", "core,diagnostics,ops")

	cfg, err := ParseArgs([]string{"http", "--bind-address", "0.0.0.0", "--port", "8080", "--read-only"})
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())

	assert.Equal(t, TransportHTTP, cfg.Transport)
	assert.Equal(t, "0.0.0.0", cfg.BindAddress)
	assert.Equal(t, 8080, cfg.Port)
	assert.True(t, cfg.HTTP1) // REST JSON default for v3.6+ compatibility
	assert.True(t, cfg.Stateless)
	assert.True(t, cfg.JSONResponse)
	assert.True(t, cfg.ReadOnly)
	assert.Equal(t, "env-inbound-secret", cfg.AuthToken)
	assert.Equal(t, []string{"core", "diagnostics", "ops"}, cfg.Toolsets)
}

func TestParseArgs_RejectsRemovedSSEAndUnauthenticatedExposedBind(t *testing.T) {
	_, err := ParseArgs([]string{"sse"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "removed")

	cfg, err := ParseArgs([]string{"http", "--bind-address", "0.0.0.0"})
	require.NoError(t, err)
	require.Error(t, cfg.Validate())
}

func TestConfig_MultiContextBoundaries(t *testing.T) {
	// HTTP + Direct K8s without allowlist disables multi-context safely by default.
	cfg, err := ParseArgs([]string{"http", "--bind-address", "127.0.0.1"})
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	assert.False(t, cfg.MultiContextEnabled())

	// HTTP + Direct K8s with explicit allowlist enables multi-context.
	cfg2, err := ParseArgs([]string{"http", "--bind-address", "127.0.0.1", "--allowed-contexts", "dev,staging"})
	require.NoError(t, err)
	require.NoError(t, cfg2.Validate())
	assert.True(t, cfg2.MultiContextEnabled())

	// Argo Server mode forbids allowed-contexts.
	cfg3, err := ParseArgs([]string{"--argo-server", "https://argo.example.com", "--allowed-contexts", "dev"})
	require.NoError(t, err)
	require.Error(t, cfg3.Validate())
}
