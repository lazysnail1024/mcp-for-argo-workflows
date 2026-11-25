package security

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeBindAddress_RejectsOctalAndShorthand(t *testing.T) {
	for _, valid := range []string{"127.0.0.1", "localhost", "0.0.0.0", "[::1]", "::1"} {
		_, err := CanonicalizeBindAddress(valid)
		assert.NoError(t, err, "expected valid bind address %q", valid)
	}
	for _, invalid := range []string{"0177.0.0.1", "127.1", "evil.example@127.0.0.1", "a/b", "*.example.com"} {
		_, err := CanonicalizeBindAddress(invalid)
		assert.Error(t, err, "expected error for %q", invalid)
	}
}

func TestResolveListenerSecurity_FailClosedAndAuthOrdering(t *testing.T) {
	// Non-loopback without auth token must fail closed.
	_, err := ResolveListenerSecurity(ListenerSecurityOptions{
		BindAddress: "0.0.0.0",
		Port:        8080,
	})
	require.Error(t, err)

	// Explicitly set empty token must fail closed.
	_, err = ResolveListenerSecurity(ListenerSecurityOptions{
		BindAddress:  "127.0.0.1",
		Port:         8080,
		AuthToken:    "   ",
		AuthTokenSet: true,
	})
	require.Error(t, err)

	// Opaque "null" origin is rejected.
	_, err = ResolveListenerSecurity(ListenerSecurityOptions{
		BindAddress:    "127.0.0.1",
		Port:           8080,
		AllowedOrigins: []string{"null"},
	})
	require.Error(t, err)

	sec, err := ResolveListenerSecurity(ListenerSecurityOptions{
		BindAddress:        "0.0.0.0",
		Port:               8080,
		AuthToken:          "secret-inbound-token",
		AuthTokenSet:       true,
		AllowedHostHeaders: []string{"devops.example.com"},
		AllowedOrigins:     []string{"vscode-webview://abc123"},
	})
	require.NoError(t, err)

	handler := sec.WrapHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Unauthenticated request with bad Host and Origin gets 401 (no allowlist enumeration).
	req := httptest.NewRequest(http.MethodPost, "http://evil.example.com/mcp", nil)
	req.Host = "evil.example.com"
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// 2. Authenticated request with bad Host gets 403.
	req2 := httptest.NewRequest(http.MethodPost, "http://evil.example.com/mcp", nil)
	req2.Host = "evil.example.com"
	req2.Header.Set("Authorization", "Bearer secret-inbound-token")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusForbidden, rec2.Code)

	// 3. Authenticated request with allowed Host and allowed vscode-webview Origin succeeds.
	req3 := httptest.NewRequest(http.MethodPost, "http://devops.example.com/mcp", nil)
	req3.Host = "devops.example.com"
	req3.Header.Set("Authorization", "Bearer secret-inbound-token")
	req3.Header.Set("Origin", "vscode-webview://abc123")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHeaderGuard_DetectsSmugglingAndBase64(t *testing.T) {
	reachedBackend := false
	guard := HeaderGuard(LegacyModeStateless, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reachedBackend = true
		w.WriteHeader(http.StatusOK)
	}))

	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_workflow","arguments":{"namespace":"argo","name":"wf-1"}}}`

	// Mismatched Mcp-Name -> 400 with -32020 and zero backend calls.
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
	req.Header.Set("Mcp-Method", "tools/call")
	req.Header.Set("Mcp-Name", "list_workflows")
	rec := httptest.NewRecorder()
	guard.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.False(t, reachedBackend)
	var rpcErr struct {
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rpcErr))
	assert.Equal(t, HeaderMismatchErrorCode, rpcErr.Error.Code)

	// Matching headers with base64-encoded Mcp-Param-Workflow-Name -> 200 OK.
	req2 := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body))
	req2.Header.Set("Mcp-Method", "tools/call")
	req2.Header.Set("Mcp-Name", "get_workflow")
	req2.Header.Set("Mcp-Param-Workflow-Name", "=?base64?"+base64.StdEncoding.EncodeToString([]byte("wf-1"))+"?=")
	rec2 := httptest.NewRecorder()
	guard.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.True(t, reachedBackend)
}
