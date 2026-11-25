// Package config handles CLI flag and environment variable parsing and validation.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"github.com/pipekit/mcp-for-argo-workflows/internal/security"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

// Supported MCP transport modes. Both "http" and "streamable-http" map to the single
// stateless Streamable HTTP /mcp endpoint (legacy dual-endpoint SSE has been removed).
const (
	TransportStdio          = "stdio"
	TransportHTTP           = "http"
	TransportStreamableHTTP = "streamable-http"

	DefaultToolsets = "core,diagnostics"
)

func isValidTransport(name string) bool {
	switch name {
	case TransportStdio, TransportHTTP, TransportStreamableHTTP:
		return true
	default:
		return false
	}
}

// Config holds full server, downstream Argo, security, and governance configuration.
type Config struct {
	// Transport & HTTP listener
	Transport            string
	HTTPAddr             string
	BindAddress          string
	Port                 int
	Stateless            bool
	JSONResponse         bool
	LegacyMode           string
	AuthToken            string
	AuthTokenSet         bool
	AllowUnauthenticated bool
	AllowedHostHeaders   []string
	AllowedOrigins       []string
	RequestStateSecret   string

	// Downstream Argo connection (pure Argo Server OR pure Direct K8s, never mixed)
	ArgoServer         string
	ArgoToken          string
	TokenRegistryPath  string
	CACert             string
	Namespace          string
	Kubeconfig         string
	Context            string
	Secure             bool
	InsecureSkipVerify bool
	HTTP1              bool
	MultiContext       bool
	AllowedContexts    []string

	// Toolset & Governance Controls
	Toolsets              []string
	Tools                 []string
	ReadOnly              bool
	AllowedNamespaces     []string
	DeniedNamespaces      []string
	AllowClusterTemplates bool
	AllowArchiveDelete    bool
	MaxResponseBytes      int

	// Internal tracking flags
	httpAddrExplicit     bool
	multiContextExplicit bool
}

// DefaultConfig returns secure-by-default production configuration.
func DefaultConfig() *Config {
	return &Config{
		Transport:        TransportStdio,
		BindAddress:      security.DefaultBindAddress,
		Port:             security.DefaultPort,
		Stateless:        true,
		JSONResponse:     true,
		LegacyMode:       security.LegacyModeStateless,
		Namespace:        "default",
		Secure:           true,
		HTTP1:            true, // REST JSON default: lossless v3.6 compat + reverse-proxy subpaths
		MultiContext:     true,
		Toolsets:         []string{"core", "diagnostics"},
		DeniedNamespaces: append([]string(nil), governance.DefaultDeniedNamespaces...),
		MaxResponseBytes: governance.DefaultMaxResponseBytes,
	}
}

// Validate normalizes and validates configuration before binding listeners or creating clients.
func (c *Config) Validate() error {
	c.Transport = strings.ToLower(strings.TrimSpace(c.Transport))
	if !isValidTransport(c.Transport) {
		return fmt.Errorf("invalid transport %q: must be %q, %q, or %q (legacy sse transport has been removed)",
			c.Transport, TransportStdio, TransportStreamableHTTP, TransportHTTP)
	}

	c.LegacyMode = strings.ToLower(strings.TrimSpace(c.LegacyMode))
	if c.LegacyMode == "" {
		c.LegacyMode = security.LegacyModeStateless
	}
	if c.LegacyMode != security.LegacyModeStateless && c.LegacyMode != security.LegacyModeReject {
		return fmt.Errorf("invalid legacy-mode %q: must be %q or %q", c.LegacyMode, security.LegacyModeStateless, security.LegacyModeReject)
	}

	if !c.Stateless {
		slog.Warn("--stateless=false is deprecated and ignored; server always runs in stateless mode for multi-replica safety")
		c.Stateless = true
	}

	if c.UsesHTTPListener() {
		if c.httpAddrExplicit && strings.TrimSpace(c.HTTPAddr) != "" {
			host, port, err := security.SplitListenAddr(c.HTTPAddr)
			if err != nil {
				return err
			}
			c.BindAddress = host
			c.Port = port
		}
		if _, err := c.ResolveListenerSecurity(); err != nil {
			return err
		}
	}

	if err := c.validateMultiContext(); err != nil {
		return err
	}

	if c.MaxResponseBytes < governance.MinMaxResponseBytes {
		return fmt.Errorf("max-response-bytes (%d) must be at least %d", c.MaxResponseBytes, governance.MinMaxResponseBytes)
	}

	return nil
}

func (c *Config) validateMultiContext() error {
	c.AllowedContexts = normalizeStringList(c.AllowedContexts)

	// In Argo Server mode, kubeconfig contexts are never used.
	if strings.TrimSpace(c.ArgoServer) != "" {
		if len(c.AllowedContexts) > 0 {
			return fmt.Errorf("allowed-contexts cannot be used in Argo Server mode; use ARGO_TOKEN_REGISTRY_PATH for multi-instance Argo Server routing")
		}
		if c.multiContextExplicit && c.MultiContext {
			return fmt.Errorf("multi-context requires Direct Kubernetes mode (empty --argo-server)")
		}
		return nil
	}

	// In HTTP + Direct K8s mode, require an explicit --allowed-contexts allowlist before
	// enabling multi-context switching to prevent unintended cluster reach over HTTP.
	if c.UsesHTTPListener() && c.multiContextExplicit && c.MultiContext && len(c.AllowedContexts) == 0 {
		return fmt.Errorf("enabling --multi-context over HTTP requires an explicit --allowed-contexts allowlist")
	}

	return nil
}

// MultiContextEnabled reports whether per-call kubeconfig context switching is active.
//   - Always false in Argo Server mode.
//   - In Direct K8s + stdio mode: enabled when MultiContext is true.
//   - In Direct K8s + HTTP mode: enabled only when MultiContext is true AND AllowedContexts is explicitly set.
func (c *Config) MultiContextEnabled() bool {
	if strings.TrimSpace(c.ArgoServer) != "" || !c.MultiContext {
		return false
	}
	if c.UsesHTTPListener() {
		return len(c.AllowedContexts) > 0
	}
	return true
}

// UsesHTTPListener reports whether the configured transport serves HTTP /mcp.
func (c *Config) UsesHTTPListener() bool {
	return c.Transport == TransportHTTP || c.Transport == TransportStreamableHTTP
}

// ResolveListenerSecurity builds validated inbound HTTP listener security policy.
func (c *Config) ResolveListenerSecurity() (*security.ListenerSecurity, error) {
	return security.ResolveListenerSecurity(security.ListenerSecurityOptions{
		BindAddress:          c.BindAddress,
		Port:                 c.Port,
		AllowedHostHeaders:   c.AllowedHostHeaders,
		AllowedOrigins:       c.AllowedOrigins,
		AuthToken:            c.AuthToken,
		AuthTokenSet:         c.AuthTokenSet,
		AllowUnauthenticated: c.AllowUnauthenticated,
	})
}

// ToArgoConfig converts Config into downstream argo.Config.
func (c *Config) ToArgoConfig() *argo.Config {
	return &argo.Config{
		ArgoServer:         c.ArgoServer,
		ArgoToken:          c.ArgoToken,
		TokenRegistryPath:  c.TokenRegistryPath,
		CACert:             c.CACert,
		Namespace:          c.Namespace,
		Kubeconfig:         c.Kubeconfig,
		Context:            c.Context,
		AllowedContexts:    c.AllowedContexts,
		Secure:             c.Secure,
		InsecureSkipVerify: c.InsecureSkipVerify,
		HTTP1:              c.HTTP1,
		MultiContext:       c.MultiContextEnabled(),
	}
}

// ToScopeFence constructs the runtime ScopeFence from Config.
func (c *Config) ToScopeFence() *governance.ScopeFence {
	return governance.NewScopeFence(governance.ScopeFenceConfig{
		AllowedNamespaces:     c.AllowedNamespaces,
		DeniedNamespaces:      c.DeniedNamespaces,
		AllowClusterTemplates: c.AllowClusterTemplates,
		AllowArchiveDelete:    c.AllowArchiveDelete,
	})
}

// NewFromFlags parses CLI flags and environment variables (CLI flags > Env vars > Defaults).
func NewFromFlags() (*Config, error) {
	return ParseArgs(os.Args[1:])
}

// ParseArgs parses explicit argv slices with a dedicated FlagSet (test-friendly).
func ParseArgs(args []string) (*Config, error) {
	cfg := DefaultConfig()
	fs := pflag.NewFlagSet("mcp-for-argo-workflows", pflag.ContinueOnError)

	// Allow optional positional subcommand "stdio" or "http" (matching argocd-mcp CLI conventions).
	fs.StringVar(&cfg.Transport, "transport", cfg.Transport, "MCP transport mode: stdio, streamable-http, or http")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", "", "HTTP listen address host:port (overrides --bind-address and --port)")
	fs.StringVar(&cfg.BindAddress, "bind-address", cfg.BindAddress, "HTTP bind address (default 127.0.0.1)")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "HTTP listen port (default 8080)")
	fs.BoolVar(&cfg.Stateless, "stateless", cfg.Stateless, "Run stateless HTTP transport (always true; kept for CLI compatibility)")
	fs.BoolVar(&cfg.JSONResponse, "json-response", cfg.JSONResponse, "Return single application/json responses instead of SSE streams")
	fs.StringVar(&cfg.LegacyMode, "legacy-mode", cfg.LegacyMode, "Protocol compatibility mode: stateless (2026-07-28 + 2025-xx) or reject (2026-07-28 only)")
	fs.StringVar(&cfg.AuthToken, "auth-token", "", "Inbound bearer token required for HTTP requests (env: MCP_AUTH_TOKEN)")
	fs.BoolVar(&cfg.AllowUnauthenticated, "allow-unauthenticated", false, "Allow binding non-loopback HTTP listener without MCP_AUTH_TOKEN")
	fs.StringSliceVar(&cfg.AllowedHostHeaders, "allowed-host-header", nil, "Allowed Host header value(s) for HTTP requests")
	fs.StringSliceVar(&cfg.AllowedOrigins, "allowed-origin", nil, "Allowed Origin header value(s) for HTTP requests")
	fs.StringVar(&cfg.RequestStateSecret, "request-state-secret", "", "Shared HMAC secret across replicas for MRTR approvals (env: MCP_REQUEST_STATE_SECRET)")

	fs.StringVar(&cfg.ArgoServer, "argo-server", "", "Argo Server URL or host:port (empty selects Direct Kubernetes mode)")
	fs.StringVar(&cfg.ArgoToken, "argo-token", "", "Bearer token for default Argo Server (env: ARGO_TOKEN)")
	fs.StringVar(&cfg.TokenRegistryPath, "argo-token-registry", "", "Path to JSON token registry file for multi-instance Argo Server routing")
	fs.StringVar(&cfg.CACert, "argo-ca-cert", "", "Path to custom CA certificate bundle for Argo Server TLS")
	fs.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "Default Kubernetes namespace")
	fs.BoolVar(&cfg.Secure, "argo-secure", cfg.Secure, "Use TLS when --argo-server omits scheme")
	fs.BoolVar(&cfg.InsecureSkipVerify, "argo-insecure-skip-verify", false, "Skip TLS verification for Argo Server (dev only)")
	fs.BoolVar(&cfg.HTTP1, "argo-http1", cfg.HTTP1, "Use REST JSON over HTTP/1.1+ for Argo Server (default true for v3.6+ compatibility)")
	fs.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Path list to kubeconfig file(s) (Direct K8s mode only)")
	fs.StringVar(&cfg.Context, "context", "", "Default kubeconfig context name (Direct K8s mode only)")
	fs.BoolVar(&cfg.MultiContext, "multi-context", cfg.MultiContext, "Allow per-call kubeconfig context selection (Direct K8s mode)")
	fs.StringSliceVar(&cfg.AllowedContexts, "allowed-contexts", nil, "Allowlist of kubeconfig contexts selectable per call")

	fs.StringSliceVar(&cfg.Toolsets, "toolsets", cfg.Toolsets, "Enabled toolsets: core, diagnostics, ops, admin, all (default: core,diagnostics)")
	fs.StringSliceVar(&cfg.Tools, "tools", nil, "Optional exact allowlist of tool names")
	fs.BoolVar(&cfg.ReadOnly, "read-only", false, "Force read-only mode, unconditionally stripping ops and admin toolsets")
	fs.StringSliceVar(&cfg.AllowedNamespaces, "allowed-namespaces", nil, "Allowlist of namespaces accessible to tools")
	fs.StringSliceVar(&cfg.DeniedNamespaces, "denied-namespaces", cfg.DeniedNamespaces, "Denylist of namespaces blocked from tools (default: kube-system)")
	fs.BoolVar(&cfg.AllowClusterTemplates, "allow-cluster-templates", false, "Allow write tools to mutate or delete ClusterWorkflowTemplates")
	fs.BoolVar(&cfg.AllowArchiveDelete, "allow-archive-delete", false, "Allow delete_resource to delete archived workflows")
	fs.IntVar(&cfg.MaxResponseBytes, "max-response-bytes", cfg.MaxResponseBytes, "Hard JSON response byte limit (default 65536)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Support positional "stdio" / "http" / "streamable-http" subcommand style (parity with argocd-mcp).
	if pos := fs.Args(); len(pos) > 0 && !fs.Changed("transport") {
		sub := strings.ToLower(strings.TrimSpace(pos[0]))
		if sub == "sse" {
			return nil, fmt.Errorf("legacy %q transport has been removed; use %q or %q", sub, TransportStreamableHTTP, TransportHTTP)
		}
		if isValidTransport(sub) {
			cfg.Transport = sub
		} else {
			return nil, fmt.Errorf("unknown positional subcommand %q: expected %q, %q, or %q", sub, TransportStdio, TransportHTTP, TransportStreamableHTTP)
		}
	}

	applyEnvOverrides(fs, cfg)
	return cfg, nil
}

func applyEnvOverrides(fs *pflag.FlagSet, cfg *Config) {
	if !fs.Changed("transport") && cfg.Transport == TransportStdio {
		if v := strings.ToLower(strings.TrimSpace(os.Getenv("MCP_TRANSPORT"))); v != "" {
			cfg.Transport = v
		}
	}

	if fs.Changed("http-addr") {
		cfg.httpAddrExplicit = true
	} else if v := strings.TrimSpace(os.Getenv("MCP_HTTP_ADDR")); v != "" && !fs.Changed("bind-address") && !fs.Changed("port") {
		cfg.HTTPAddr = v
		cfg.httpAddrExplicit = true
	}

	cfg.BindAddress = getEnvString(fs, "bind-address", "MCP_BIND_ADDRESS", cfg.BindAddress)
	cfg.Port = getEnvInt(fs, "port", "MCP_PORT", cfg.Port)
	cfg.Stateless = getEnvBool(fs, "stateless", "MCP_STATELESS", cfg.Stateless)
	cfg.JSONResponse = getEnvBool(fs, "json-response", "MCP_JSON_RESPONSE", cfg.JSONResponse)
	cfg.LegacyMode = getEnvString(fs, "legacy-mode", "MCP_LEGACY_MODE", cfg.LegacyMode)

	if fs.Changed("auth-token") {
		cfg.AuthTokenSet = true
	} else if v, exists := os.LookupEnv("MCP_AUTH_TOKEN"); exists {
		cfg.AuthToken = v
		cfg.AuthTokenSet = true
	}
	cfg.AllowUnauthenticated = getEnvBool(fs, "allow-unauthenticated", "MCP_ALLOW_UNAUTHENTICATED", cfg.AllowUnauthenticated)
	cfg.AllowedHostHeaders = getEnvSlice(fs, "allowed-host-header", "MCP_ALLOWED_HOST_HEADERS", cfg.AllowedHostHeaders)
	cfg.AllowedOrigins = getEnvSlice(fs, "allowed-origin", "MCP_ALLOWED_ORIGINS", cfg.AllowedOrigins)
	cfg.RequestStateSecret = getEnvString(fs, "request-state-secret", "MCP_REQUEST_STATE_SECRET", cfg.RequestStateSecret)

	cfg.ArgoServer = getEnvString(fs, "argo-server", "ARGO_SERVER", cfg.ArgoServer)
	cfg.ArgoToken = getEnvString(fs, "argo-token", "ARGO_TOKEN", cfg.ArgoToken)
	cfg.TokenRegistryPath = getEnvString(fs, "argo-token-registry", "ARGO_TOKEN_REGISTRY_PATH", cfg.TokenRegistryPath)
	cfg.CACert = getEnvString(fs, "argo-ca-cert", "ARGO_CA_CERT", cfg.CACert)
	cfg.Namespace = getEnvString(fs, "namespace", "ARGO_NAMESPACE", cfg.Namespace)
	cfg.Kubeconfig = getEnvString(fs, "kubeconfig", "KUBECONFIG", cfg.Kubeconfig)
	cfg.Secure = getEnvBool(fs, "argo-secure", "ARGO_SECURE", cfg.Secure)
	cfg.InsecureSkipVerify = getEnvBool(fs, "argo-insecure-skip-verify", "ARGO_INSECURE_SKIP_VERIFY", cfg.InsecureSkipVerify)
	cfg.HTTP1 = getEnvBool(fs, "argo-http1", "ARGO_HTTP1", cfg.HTTP1)
	cfg.MultiContext = getEnvBool(fs, "multi-context", "MCP_MULTI_CONTEXT", cfg.MultiContext)
	cfg.multiContextExplicit = fs.Changed("multi-context") || os.Getenv("MCP_MULTI_CONTEXT") != ""
	cfg.AllowedContexts = getEnvSlice(fs, "allowed-contexts", "MCP_ALLOWED_CONTEXTS", cfg.AllowedContexts)

	cfg.Toolsets = getEnvSlice(fs, "toolsets", "MCP_TOOLSETS", cfg.Toolsets)
	cfg.Tools = getEnvSlice(fs, "tools", "MCP_TOOLS", cfg.Tools)
	cfg.ReadOnly = getEnvBool(fs, "read-only", "MCP_READ_ONLY", cfg.ReadOnly)
	cfg.AllowedNamespaces = getEnvSlice(fs, "allowed-namespaces", "MCP_ALLOWED_NAMESPACES", cfg.AllowedNamespaces)
	if !fs.Changed("denied-namespaces") {
		if raw, exists := os.LookupEnv("MCP_DENIED_NAMESPACES"); exists {
			cfg.DeniedNamespaces = normalizeStringList(strings.Split(raw, ","))
		}
	}
	cfg.AllowClusterTemplates = getEnvBool(fs, "allow-cluster-templates", "MCP_ALLOW_CLUSTER_TEMPLATES", cfg.AllowClusterTemplates)
	cfg.AllowArchiveDelete = getEnvBool(fs, "allow-archive-delete", "MCP_ALLOW_ARCHIVE_DELETE", cfg.AllowArchiveDelete)
	cfg.MaxResponseBytes = getEnvInt(fs, "max-response-bytes", "MCP_MAX_RESPONSE_BYTES", cfg.MaxResponseBytes)
}

func getEnvString(fs *pflag.FlagSet, flagName, envKey, current string) string {
	if !fs.Changed(flagName) {
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			return v
		}
	}
	return current
}

func getEnvBool(fs *pflag.FlagSet, flagName, envKey string, current bool) bool {
	if !fs.Changed(flagName) {
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				return b
			}
			slog.Warn("invalid boolean env var, using default", "env", envKey, "value", strconv.Quote(v), "default", current)
		}
	}
	return current
}

func getEnvInt(fs *pflag.FlagSet, flagName, envKey string, current int) int {
	if !fs.Changed(flagName) {
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
			slog.Warn("invalid integer env var, using default", "env", envKey, "value", strconv.Quote(v), "default", current)
		}
	}
	return current
}

func getEnvSlice(fs *pflag.FlagSet, flagName, envKey string, current []string) []string {
	if !fs.Changed(flagName) {
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			return normalizeStringList(strings.Split(v, ","))
		}
	}
	return normalizeStringList(current)
}

func normalizeStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, raw := range in {
		for _, part := range strings.Split(raw, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
