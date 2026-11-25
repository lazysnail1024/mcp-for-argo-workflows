package argo

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// pathSegmentPattern enforces safe single-segment Kubernetes / Argo resource names
// and UUIDs, blocking path traversal payloads (../, /, \, %, whitespace).
var pathSegmentPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,252}$`)

// ParsedArgoURL holds a canonicalized Argo Server endpoint broken into the pieces
// required by apiclient.ArgoServerOpts and TokenRegistry.
type ParsedArgoURL struct {
	NormalizedBaseURL string
	HostPort          string
	Path              string
	Secure            bool
}

// ValidatePathSegment validates a Kubernetes/Argo resource identifier (name, namespace, uid)
// before interpolation into Argo Server REST paths or API calls.
func ValidatePathSegment(value, fieldName string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("%s cannot be empty", fieldName)
	}
	if trimmed == "." || trimmed == ".." ||
		strings.ContainsAny(trimmed, "/\\% \t\r\n?#") ||
		strings.Contains(trimmed, "..") {
		return fmt.Errorf("invalid %s %q: contains forbidden path traversal or separator characters", fieldName, value)
	}
	if !pathSegmentPattern.MatchString(trimmed) {
		return fmt.Errorf("invalid %s %q: must match %s", fieldName, value, pathSegmentPattern.String())
	}
	return nil
}

// ParseArgoServerURL parses either a full URL ("https://argo.example.com/argo",
// "http://argo-server.argo.svc.cluster.local:2746") or a bare host:port[/path]
// ("argo-server.argo:2746") into canonical components preserving reverse-proxy subpaths.
func ParseArgoServerURL(raw string, defaultSecure bool) (ParsedArgoURL, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ParsedArgoURL{}, fmt.Errorf("argo server URL cannot be empty")
	}

	candidate := trimmed
	if !strings.Contains(candidate, "://") {
		scheme := "http"
		if defaultSecure {
			scheme = "https"
		}
		candidate = scheme + "://" + candidate
	}

	u, err := url.Parse(candidate)
	if err != nil {
		return ParsedArgoURL{}, fmt.Errorf("invalid Argo Server URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ParsedArgoURL{}, fmt.Errorf("invalid Argo Server URL %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return ParsedArgoURL{}, fmt.Errorf("invalid Argo Server URL %q: host is required", raw)
	}
	if u.User != nil {
		return ParsedArgoURL{}, fmt.Errorf("invalid Argo Server URL %q: embedded userinfo credentials are not permitted", raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return ParsedArgoURL{}, fmt.Errorf("invalid Argo Server URL %q: query strings and fragments are not permitted", raw)
	}

	cleanPath := strings.TrimRight(u.Path, "/")
	if strings.Contains(cleanPath, "..") {
		return ParsedArgoURL{}, fmt.Errorf("invalid Argo Server URL %q: path traversal is not permitted", raw)
	}

	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Host)
	normalized := fmt.Sprintf("%s://%s%s", scheme, host, cleanPath)

	return ParsedArgoURL{
		NormalizedBaseURL: normalized,
		HostPort:          host,
		Path:              cleanPath,
		Secure:            scheme == "https",
	}, nil
}

// NormalizeBaseURL normalizes an Argo Server URL for registry map keys.
func NormalizeBaseURL(raw string, defaultSecure bool) string {
	parsed, err := ParseArgoServerURL(raw, defaultSecure)
	if err != nil {
		return strings.TrimRight(strings.TrimSpace(raw), "/")
	}
	return parsed.NormalizedBaseURL
}

// NormalizeBearerToken normalizes an Argo Server authentication token so operators
// can pass either a raw JWT ("eyJ...") or a prefixed value ("Bearer eyJ..." / "v2:...").
func NormalizeBearerToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "bearer ") || strings.HasPrefix(lower, "v2:") {
		return trimmed
	}
	return "Bearer " + trimmed
}
