package argo

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatePathSegment(t *testing.T) {
	assert.NoError(t, ValidatePathSegment("my-workflow-123", "name"))
	assert.NoError(t, ValidatePathSegment("550e8400-e29b-41d4-a716-446655440000", "uid"))

	for _, bad := range []string{"", ".", "..", "../clusters", "a/b", `a\b`, "wf%2e%2e", "bad name", "wf?x=1"} {
		assert.Error(t, ValidatePathSegment(bad, "name"), "expected error for %q", bad)
	}
}

func TestParseArgoServerURL(t *testing.T) {
	p, err := ParseArgoServerURL("https://devops.example.com/argo/", false)
	require.NoError(t, err)
	assert.Equal(t, "https://devops.example.com/argo", p.NormalizedBaseURL)
	assert.Equal(t, "devops.example.com", p.HostPort)
	assert.Equal(t, "/argo", p.Path)
	assert.True(t, p.Secure)

	p2, err := ParseArgoServerURL("http://argo-server.argo.svc.cluster.local:2746", true)
	require.NoError(t, err)
	assert.Equal(t, "http://argo-server.argo.svc.cluster.local:2746", p2.NormalizedBaseURL)
	assert.False(t, p2.Secure)

	p3, err := ParseArgoServerURL("argo-server.argo:2746", true)
	require.NoError(t, err)
	assert.Equal(t, "https://argo-server.argo:2746", p3.NormalizedBaseURL)

	_, err = ParseArgoServerURL("https://user:pass@argo.example.com", true)
	assert.Error(t, err)
	_, err = ParseArgoServerURL("https://argo.example.com/a/../b", true)
	assert.Error(t, err)
}

func TestNormalizeBearerToken(t *testing.T) {
	assert.Equal(t, "", NormalizeBearerToken(""))
	assert.Equal(t, "Bearer eyJabc", NormalizeBearerToken("eyJabc"))
	assert.Equal(t, "Bearer eyJabc", NormalizeBearerToken("Bearer eyJabc"))
	assert.Equal(t, "v2:session", NormalizeBearerToken("v2:session"))
}

func TestTokenRegistry_HotReloadAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")

	// Missing file fails closed.
	_, err := LoadTokenRegistryFromFile(filepath.Join(dir, "missing.json"))
	require.Error(t, err)

	// Valid initial file loads cleanly.
	require.NoError(t, os.WriteFile(path, []byte(`[{"baseUrl":"https://argo-b.example.com/argo/","token":"tok-1"}]`), 0o600))
	reg, err := LoadTokenRegistryFromFile(path, WithReloadCheckInterval(0))
	require.NoError(t, err)
	tok, ok := reg.GetToken("https://argo-b.example.com/argo")
	require.True(t, ok)
	assert.Equal(t, "Bearer tok-1", tok)

	// Rotate file with newer mtime -> hot reloads automatically.
	future := time.Now().Add(2 * time.Second)
	require.NoError(t, os.WriteFile(path, []byte(`[{"baseUrl":"https://argo-b.example.com/argo","token":"tok-2"}]`), 0o600))
	require.NoError(t, os.Chtimes(path, future, future))
	tok2, ok := reg.GetToken("https://argo-b.example.com/argo")
	require.True(t, ok)
	assert.Equal(t, "Bearer tok-2", tok2)

	// Transient corrupt write during rotation preserves last known good token.
	future2 := future.Add(2 * time.Second)
	require.NoError(t, os.WriteFile(path, []byte(`{corrupt`), 0o600))
	require.NoError(t, os.Chtimes(path, future2, future2))
	tok3, ok := reg.GetToken("https://argo-b.example.com/argo")
	require.True(t, ok)
	assert.Equal(t, "Bearer tok-2", tok3)
}
