// Package security implements inbound HTTP listener hardening modeled on argocd-mcp:
// loopback default bind, fail-closed bearer authentication, DNS rebinding protection
// (Host and Origin validation), and cross-era SEP-2243 MCP header-desync verification.
package security

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	// DefaultBindAddress binds locally unless operators explicitly widen network exposure.
	DefaultBindAddress = "127.0.0.1"
	// DefaultPort is the default HTTP port for streamable HTTP transport.
	DefaultPort = 8080

	jsonRPCInvalidRequestCode = -32600
)

var (
	loopbackHostnames = []string{"localhost", "127.0.0.1", "[::1]"}
	hostnameLabelRe   = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)
	opaqueHostRe      = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	digitsOnlyRe      = regexp.MustCompile(`^\d+$`)
)

// ListenerSecurityOptions holds operator-supplied listener exposure configuration.
type ListenerSecurityOptions struct {
	BindAddress          string
	Port                 int
	AllowedHostHeaders   []string
	AllowedOrigins       []string
	AuthToken            string
	AuthTokenSet         bool
	AllowUnauthenticated bool
}

// ListenerSecurity contains validated, normalized runtime security policies.
type ListenerSecurity struct {
	BindAddress        string
	Port               int
	ListenAddr         string
	AllowedHostnames   map[string]struct{}
	AllowedOrigins     map[string]struct{}
	AuthToken          string
	ValidateHostHeader bool
}

// SplitListenAddr splits an http-addr string (e.g. ":8080", "127.0.0.1:8080", "[::1]:8080")
// into bare host and numeric port.
func SplitListenAddr(addr string) (host string, port int, err error) {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return DefaultBindAddress, DefaultPort, nil
	}
	h, pStr, splitErr := net.SplitHostPort(trimmed)
	if splitErr != nil {
		return "", 0, fmt.Errorf("invalid --http-addr %q: %w", addr, splitErr)
	}
	p, convErr := strconv.Atoi(pStr)
	if convErr != nil || p < 0 || p > 65535 {
		return "", 0, fmt.Errorf("invalid port in --http-addr %q", addr)
	}
	if h == "" {
		h = "0.0.0.0"
	}
	return h, p, nil
}

// CanonicalizeBindAddress validates a bind host/IP, rejecting octal/shorthand IPs
// and userinfo-smuggled values.
func CanonicalizeBindAddress(address string) (string, error) {
	canonical := strings.ToLower(strings.TrimSpace(address))
	if canonical == "" {
		canonical = DefaultBindAddress
	}
	if !isUsableHostname(canonical) {
		return "", fmt.Errorf(
			"invalid bind address %q: expected an IP address or hostname such as %s, 0.0.0.0, [::1], or mcp.internal.example.com",
			address, DefaultBindAddress,
		)
	}
	return canonical, nil
}

// NormalizeBindAddress strips IPv6 brackets for net.Listen.
func NormalizeBindAddress(address string) string {
	if strings.HasPrefix(address, "[") && strings.HasSuffix(address, "]") {
		return address[1 : len(address)-1]
	}
	return address
}

// IsLoopbackAddress reports whether address resolves strictly to loopback (localhost, 127.0.0.0/8, ::1).
func IsLoopbackAddress(address string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(address))
	bare := NormalizeBindAddress(trimmed)
	if bare == "localhost" {
		return true
	}
	if ip, err := netip.ParseAddr(bare); err == nil {
		return ip.IsLoopback()
	}
	return false
}

func isWildcardBindAddress(address string) bool {
	bare := NormalizeBindAddress(strings.ToLower(strings.TrimSpace(address)))
	return bare == "0.0.0.0" || bare == "::"
}

func bracketIfIPv6(host string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]"
	}
	return host
}

func isUsableHostname(hostname string) bool {
	if hostname == "" || len(hostname) > 253 {
		return false
	}
	bare := NormalizeBindAddress(hostname)
	if bare != hostname {
		ip, err := netip.ParseAddr(bare)
		return err == nil && ip.Is6()
	}
	if _, err := netip.ParseAddr(hostname); err == nil {
		return true
	}
	labels := strings.Split(hostname, ".")
	// Reject mistyped/octal IPs such as 0177.0.0.1 or 127.1 whose final label is all digits.
	if digitsOnlyRe.MatchString(labels[len(labels)-1]) {
		return false
	}
	for _, label := range labels {
		if !hostnameLabelRe.MatchString(label) {
			return false
		}
	}
	return true
}

func strictHostname(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	candidate := trimmed
	if !strings.Contains(candidate, "://") {
		candidate = "http://" + candidate
	}
	u, err := url.Parse(candidate)
	if err != nil || u.User != nil || u.Host == "" {
		return ""
	}
	if u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return ""
	}
	hostOnly := strings.ToLower(u.Hostname())
	if hostOnly == "" {
		return ""
	}
	if strings.Contains(hostOnly, ":") {
		hostOnly = "[" + hostOnly + "]"
	}
	if !isUsableHostname(hostOnly) {
		return ""
	}
	return hostOnly
}

func canonicalOrigin(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "null") {
		return ""
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return ""
	}
	if u.Path != "" && u.Path != "/" {
		return ""
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return ""
	}

	scheme := strings.ToLower(u.Scheme)
	hostname := strings.ToLower(u.Hostname())
	port := u.Port()

	switch scheme {
	case "http", "https":
		hostKey := hostname
		if strings.Contains(hostKey, ":") {
			hostKey = "[" + hostKey + "]"
		}
		if !isUsableHostname(hostKey) {
			return ""
		}
		// Normalize default ports per WHATWG origin serialization.
		if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
			port = ""
		}
		if port != "" {
			return fmt.Sprintf("%s://%s:%s", scheme, hostKey, port)
		}
		return fmt.Sprintf("%s://%s", scheme, hostKey)
	default:
		// Support non-special IDE origins such as vscode-webview://<id> or chrome-extension://<id>.
		if !opaqueHostRe.MatchString(hostname) {
			return ""
		}
		if port != "" {
			return fmt.Sprintf("%s://%s:%s", scheme, hostname, port)
		}
		return fmt.Sprintf("%s://%s", scheme, hostname)
	}
}

// ResolveListenerSecurity validates listener exposure and fails closed before binding any socket.
func ResolveListenerSecurity(opts ListenerSecurityOptions) (*ListenerSecurity, error) {
	canonicalBind, err := CanonicalizeBindAddress(opts.BindAddress)
	if err != nil {
		return nil, err
	}

	if opts.AuthTokenSet && strings.TrimSpace(opts.AuthToken) == "" {
		return nil, fmt.Errorf("MCP_AUTH_TOKEN is set but empty; provide a valid bearer token or unset it")
	}

	authToken := strings.TrimSpace(opts.AuthToken)
	if authToken != "" && strings.ContainsAny(authToken, " \t\r\n") {
		return nil, fmt.Errorf("MCP_AUTH_TOKEN contains whitespace, which cannot be transmitted in an Authorization header")
	}

	if !IsLoopbackAddress(canonicalBind) && authToken == "" && !opts.AllowUnauthenticated {
		return nil, fmt.Errorf(
			"refusing to bind to non-loopback address %s without inbound authentication; "+
				"set MCP_AUTH_TOKEN to require 'Authorization: Bearer <token>', pass --allow-unauthenticated "+
				"if protected by an external proxy/mesh, or bind to %s",
			canonicalBind, DefaultBindAddress,
		)
	}

	allowedHosts := make(map[string]struct{}, len(loopbackHostnames)+len(opts.AllowedHostHeaders)+1)
	for _, h := range loopbackHostnames {
		allowedHosts[h] = struct{}{}
	}
	for _, rawHost := range opts.AllowedHostHeaders {
		h := strictHostname(rawHost)
		if h == "" {
			return nil, fmt.Errorf(
				"invalid --allowed-host-header value %q: expected a single hostname, host:port, or IP (no wildcards or paths)",
				rawHost,
			)
		}
		allowedHosts[h] = struct{}{}
	}
	if !isWildcardBindAddress(canonicalBind) {
		allowedHosts[bracketIfIPv6(NormalizeBindAddress(canonicalBind))] = struct{}{}
	}

	allowedOrigins := make(map[string]struct{}, len(loopbackHostnames)*2+len(opts.AllowedOrigins))
	for _, h := range loopbackHostnames {
		for _, scheme := range []string{"http", "https"} {
			if opts.Port > 0 {
				allowedOrigins[fmt.Sprintf("%s://%s:%d", scheme, h, opts.Port)] = struct{}{}
			}
			allowedOrigins[fmt.Sprintf("%s://%s", scheme, h)] = struct{}{}
		}
	}
	for _, rawOrigin := range opts.AllowedOrigins {
		if strings.EqualFold(strings.TrimSpace(rawOrigin), "null") {
			return nil, fmt.Errorf(`invalid --allowed-origin value %q: opaque "null" origins cannot be allowlisted`, rawOrigin)
		}
		candidate := strings.TrimSpace(rawOrigin)
		if !strings.Contains(candidate, "://") {
			candidate = "https://" + candidate
		}
		norm := canonicalOrigin(candidate)
		if norm == "" {
			return nil, fmt.Errorf("invalid --allowed-origin value %q: expected an origin like https://ide.example.com or vscode-webview://<id>", rawOrigin)
		}
		allowedOrigins[norm] = struct{}{}
	}

	bareBind := NormalizeBindAddress(canonicalBind)
	sec := &ListenerSecurity{
		BindAddress:        bareBind,
		Port:               opts.Port,
		ListenAddr:         net.JoinHostPort(bareBind, strconv.Itoa(opts.Port)),
		AllowedHostnames:   allowedHosts,
		AllowedOrigins:     allowedOrigins,
		AuthToken:          authToken,
		ValidateHostHeader: IsLoopbackAddress(canonicalBind) || len(opts.AllowedHostHeaders) > 0,
	}

	if !sec.ValidateHostHeader {
		slog.Warn("Host header validation is disabled on non-loopback bind without --allowed-host-header; pass --allowed-host-header <hostname> in production",
			"bindAddress", bareBind)
	}
	if sec.AuthToken == "" && !IsLoopbackAddress(canonicalBind) {
		slog.Warn("listener binds non-loopback address with --allow-unauthenticated; ensure external network/proxy authentication protects this endpoint",
			"bindAddress", bareBind)
	}

	return sec, nil
}

// WrapHandler applies inbound security guards in strict fail-closed order:
// 1) BearerAuth (first, preventing unauthenticated Host/Origin enumeration)
// 2) HostHeaderValidation
// 3) OriginValidation
func (s *ListenerSecurity) WrapHandler(next http.Handler) http.Handler {
	h := next
	h = originValidationMiddleware(s.AllowedOrigins, h)
	if s.ValidateHostHeader {
		h = hostValidationMiddleware(s.AllowedHostnames, h)
	}
	if s.AuthToken != "" {
		h = bearerAuthMiddleware(s.AuthToken, h)
	}
	return h
}

func bearerAuthMiddleware(expectedToken string, next http.Handler) http.Handler {
	expectedSum := sha256.Sum256([]byte(expectedToken))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fields := strings.Fields(strings.TrimSpace(r.Header.Get("Authorization")))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "bearer") {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSONRPCError(w, http.StatusUnauthorized, jsonRPCInvalidRequestCode, "Missing or invalid inbound bearer token")
			return
		}
		presentedSum := sha256.Sum256([]byte(fields[1]))
		if subtle.ConstantTimeCompare(presentedSum[:], expectedSum[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSONRPCError(w, http.StatusUnauthorized, jsonRPCInvalidRequestCode, "Missing or invalid inbound bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func hostValidationMiddleware(allowedHosts map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := strictHostname(r.Host)
		if host == "" {
			writeJSONRPCError(w, http.StatusForbidden, jsonRPCInvalidRequestCode, fmt.Sprintf("Invalid Host header: %q", r.Host))
			return
		}
		if _, ok := allowedHosts[host]; !ok {
			writeJSONRPCError(w, http.StatusForbidden, jsonRPCInvalidRequestCode, fmt.Sprintf("Invalid Host header: %q. Configure --allowed-host-header to allow it.", r.Host))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func originValidationMiddleware(allowedOrigins map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		norm := canonicalOrigin(origin)
		if norm == "" {
			writeJSONRPCError(w, http.StatusForbidden, jsonRPCInvalidRequestCode, fmt.Sprintf("Invalid Origin: %q", origin))
			return
		}
		if _, ok := allowedOrigins[norm]; !ok {
			writeJSONRPCError(w, http.StatusForbidden, jsonRPCInvalidRequestCode, fmt.Sprintf("Invalid Origin: %q. Configure --allowed-origin to allow it.", origin))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSONRPCError(w http.ResponseWriter, statusCode int, rpcCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"error": map[string]any{
			"code":    rpcCode,
			"message": message,
		},
		"id": nil,
	})
}
