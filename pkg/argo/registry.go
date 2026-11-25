package argo

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultReloadCheckInterval = 2 * time.Second

// TokenRegistryEntry maps an Argo Server base URL to its host-bound bearer token.
type TokenRegistryEntry struct {
	BaseURL string `json:"baseUrl"`
	Token   string `json:"token"`
}

// TokenRegistry stores host-bound outbound Argo Server tokens loaded from
// ARGO_TOKEN_REGISTRY_PATH with mtime-based hot reload for Secret rotation.
type TokenRegistry struct {
	mu                  sync.RWMutex
	tokensByBaseURL     map[string]string
	sourcePath          string
	lastModTime         time.Time
	lastCheckedAt       time.Time
	reloadCheckInterval time.Duration
	defaultSecure       bool
}

// TokenRegistryOption configures optional behavior of TokenRegistry.
type TokenRegistryOption func(*TokenRegistry)

// WithReloadCheckInterval configures how frequently file mtime is polled on lookups.
func WithReloadCheckInterval(d time.Duration) TokenRegistryOption {
	return func(r *TokenRegistry) {
		r.reloadCheckInterval = d
	}
}

// WithDefaultSecure configures the fallback scheme for bare host:port entries.
func WithDefaultSecure(secure bool) TokenRegistryOption {
	return func(r *TokenRegistry) {
		r.defaultSecure = secure
	}
}

// NewTokenRegistry builds an in-memory TokenRegistry from explicit entries.
func NewTokenRegistry(entries []TokenRegistryEntry, opts ...TokenRegistryOption) (*TokenRegistry, error) {
	r := &TokenRegistry{
		reloadCheckInterval: defaultReloadCheckInterval,
		defaultSecure:       true,
		lastCheckedAt:       time.Now(),
	}
	for _, opt := range opts {
		opt(r)
	}
	m, err := buildRegistryMap(entries, r.defaultSecure)
	if err != nil {
		return nil, err
	}
	r.tokensByBaseURL = m
	return r, nil
}

// LoadTokenRegistryFromFile loads a TokenRegistry from path with fail-closed validation.
// If path is empty, returns an empty registry without error.
func LoadTokenRegistryFromFile(path string, opts ...TokenRegistryOption) (*TokenRegistry, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return NewTokenRegistry(nil, opts...)
	}

	info, err := os.Stat(trimmed)
	if err != nil {
		return nil, fmt.Errorf("failed to stat Argo token registry file %q: %w", trimmed, err)
	}
	raw, err := os.ReadFile(trimmed) //nolint:gosec // Operator-configured credential registry path
	if err != nil {
		return nil, fmt.Errorf("failed to read Argo token registry file %q: %w", trimmed, err)
	}

	entries, err := parseTokenRegistryJSON(raw)
	if err != nil {
		return nil, err
	}

	r, err := NewTokenRegistry(entries, opts...)
	if err != nil {
		return nil, err
	}
	r.sourcePath = trimmed
	r.lastModTime = info.ModTime()
	r.lastCheckedAt = time.Now()

	slog.Info("loaded Argo token registry", "path", trimmed, "entries", len(r.tokensByBaseURL))
	return r, nil
}

func parseTokenRegistryJSON(raw []byte) ([]TokenRegistryEntry, error) {
	var entries []TokenRegistryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("Argo token registry file is not a valid JSON array: %w", err)
	}
	return entries, nil
}

func buildRegistryMap(entries []TokenRegistryEntry, defaultSecure bool) (map[string]string, error) {
	m := make(map[string]string, len(entries))
	for i, entry := range entries {
		base := strings.TrimSpace(entry.BaseURL)
		token := strings.TrimSpace(entry.Token)
		if base == "" || token == "" {
			return nil, fmt.Errorf("Argo token registry entry[%d] is missing baseUrl or token", i)
		}
		parsed, err := ParseArgoServerURL(base, defaultSecure)
		if err != nil {
			return nil, fmt.Errorf("Argo token registry entry[%d] has invalid baseUrl: %w", i, err)
		}
		m[parsed.NormalizedBaseURL] = NormalizeBearerToken(token)
	}
	return m, nil
}

func (r *TokenRegistry) maybeReload() {
	if r == nil || r.sourcePath == "" {
		return
	}

	r.mu.RLock()
	shouldCheck := r.reloadCheckInterval <= 0 || time.Since(r.lastCheckedAt) >= r.reloadCheckInterval
	r.mu.RUnlock()
	if !shouldCheck {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reloadCheckInterval > 0 && time.Since(r.lastCheckedAt) < r.reloadCheckInterval {
		return
	}
	r.lastCheckedAt = time.Now()

	info, err := os.Stat(r.sourcePath)
	if err != nil {
		slog.Warn("failed to stat Argo token registry during hot-reload; preserving previous entries",
			"path", r.sourcePath, "error", err)
		return
	}
	if !info.ModTime().After(r.lastModTime) && info.ModTime().Equal(r.lastModTime) {
		return
	}

	raw, err := os.ReadFile(r.sourcePath) //nolint:gosec // Operator-configured credential registry path
	if err != nil {
		slog.Warn("failed to read Argo token registry during hot-reload; preserving previous entries",
			"path", r.sourcePath, "error", err)
		return
	}
	entries, err := parseTokenRegistryJSON(raw)
	if err != nil {
		slog.Warn("failed to parse Argo token registry during hot-reload; preserving previous entries",
			"path", r.sourcePath, "error", err)
		return
	}
	rebuilt, err := buildRegistryMap(entries, r.defaultSecure)
	if err != nil {
		slog.Warn("invalid Argo token registry during hot-reload; preserving previous entries",
			"path", r.sourcePath, "error", err)
		return
	}

	r.tokensByBaseURL = rebuilt
	r.lastModTime = info.ModTime()
	slog.Info("reloaded Argo token registry", "path", r.sourcePath, "entries", len(rebuilt))
}

// GetToken looks up the host-bound bearer token for baseURL.
func (r *TokenRegistry) GetToken(baseURL string) (string, bool) {
	if r == nil || strings.TrimSpace(baseURL) == "" {
		return "", false
	}
	r.maybeReload()
	key := NormalizeBaseURL(baseURL, r.defaultSecure)

	r.mu.RLock()
	defer r.mu.RUnlock()
	tok, ok := r.tokensByBaseURL[key]
	return tok, ok
}

// ListBaseURLs returns all registered normalized base URLs sorted deterministically.
func (r *TokenRegistry) ListBaseURLs() []string {
	if r == nil {
		return nil
	}
	r.maybeReload()

	r.mu.RLock()
	defer r.mu.RUnlock()
	urls := make([]string, 0, len(r.tokensByBaseURL))
	for u := range r.tokensByBaseURL {
		urls = append(urls, u)
	}
	sort.Strings(urls)
	return urls
}

// Size returns the number of registered Argo Server instances.
func (r *TokenRegistry) Size() int {
	if r == nil {
		return 0
	}
	r.maybeReload()

	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tokensByBaseURL)
}
