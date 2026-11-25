package argo

import (
	"log/slog"
	"os"
	"strconv"
)

// defaultNamespace is the namespace assumed when none is configured.
const defaultNamespace = "default"

// Config holds the configuration for connecting to Argo Workflows in either
// pure Argo Server mode or pure Direct Kubernetes API mode (never mixed).
type Config struct {
	// ArgoServer is the default Argo Server endpoint (full URL with optional
	// reverse-proxy subpath or bare host:port). Empty selects Direct Kubernetes mode.
	ArgoServer string

	// ArgoToken is the bearer token bound strictly to ArgoServer.
	ArgoToken string

	// TokenRegistryPath points to a JSON file mapping additional Argo Server base URLs
	// to their dedicated bearer tokens (supports mtime hot-reload).
	TokenRegistryPath string

	// CACert is an optional custom CA bundle path for verifying Argo Server TLS certificates.
	// Go's standard library also honors SSL_CERT_FILE automatically.
	CACert string

	// Namespace is the fallback default namespace for operations.
	Namespace string

	// Kubeconfig is the path list to kubeconfig file(s) (Direct K8s mode only).
	Kubeconfig string

	// Context is the default kubeconfig context name (Direct K8s mode only).
	Context string

	// AllowedContexts limits per-call context selection in Direct K8s mode.
	AllowedContexts []string

	// Secure controls default TLS usage when ArgoServer omits http:// or https://.
	Secure bool

	// InsecureSkipVerify skips TLS certificate verification (development only).
	InsecureSkipVerify bool

	// HTTP1 selects REST JSON (HTTP/1.1+) instead of gRPC for Argo Server mode.
	// Defaults to true so v3.6 legacy fields are losslessly normalized and reverse-proxy
	// subpaths work out of the box.
	HTTP1 bool

	// MultiContext allows per-call kubeconfig context selection in Direct K8s mode.
	MultiContext bool
}

// NewConfigFromEnv creates a Config from environment variables.
func NewConfigFromEnv() *Config {
	cfg := &Config{
		ArgoServer:        os.Getenv("ARGO_SERVER"),
		ArgoToken:         os.Getenv("ARGO_TOKEN"),
		TokenRegistryPath: os.Getenv("ARGO_TOKEN_REGISTRY_PATH"),
		CACert:            os.Getenv("ARGO_CA_CERT"),
		Namespace:         os.Getenv("ARGO_NAMESPACE"),
		Kubeconfig:        os.Getenv("KUBECONFIG"),
		Secure:            true,
		HTTP1:             true,
	}

	if v := os.Getenv("ARGO_SECURE"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.Secure = b
		} else {
			slog.Warn("invalid ARGO_SECURE value, using default", "value", strconv.Quote(v), "fallback", true)
		}
	}
	if v := os.Getenv("ARGO_INSECURE_SKIP_VERIFY"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.InsecureSkipVerify = b
		} else {
			slog.Warn("invalid ARGO_INSECURE_SKIP_VERIFY value, using default", "value", strconv.Quote(v), "fallback", false)
		}
	}
	if v := os.Getenv("ARGO_HTTP1"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.HTTP1 = b
		} else {
			slog.Warn("invalid ARGO_HTTP1 value, using default", "value", strconv.Quote(v), "fallback", true)
		}
	}
	if cfg.Namespace == "" {
		cfg.Namespace = defaultNamespace
	}
	return cfg
}
