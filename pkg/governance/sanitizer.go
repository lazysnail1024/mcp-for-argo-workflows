// Package governance implements context engineering, redaction, scoping, and
// destructive-action confirmation guardrails for MCP tool execution.
package governance

import (
	"regexp"
	"strings"
)

const redactedPlaceholder = "[REDACTED]"

var (
	sensitiveKeyPattern = regexp.MustCompile(`(?i)(^|[._-])(token|secret|password|passwd|pwd|authorization|auth[-_]?header|api[-_]?key|access[-_]?key|private[-_]?key|client[-_]?secret|credential|kubeconfig)([._-]|$)`)
	bearerTokenPattern  = regexp.MustCompile(`(?i)\b(Bearer\s+)[A-Za-z0-9._~+/=-]{12,}`)
	basicAuthPattern    = regexp.MustCompile(`(?i)\b(Basic\s+)[A-Za-z0-9+/=]{8,}`)
	inlineSecretPattern = regexp.MustCompile(`(?i)\b(AWS_SECRET_ACCESS_KEY|ARGO_TOKEN|MCP_AUTH_TOKEN|GITHUB_TOKEN|API_KEY|ACCESS_TOKEN|PASSWORD)\s*[:=]\s*([^\s"',;]{4,})`)
	pemBlockPattern     = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----`)
)

// noisyAnnotations are Kubernetes machine annotations stripped to save context tokens.
var noisyAnnotations = map[string]struct{}{
	"kubectl.kubernetes.io/last-applied-configuration": {},
	"workflows.argoproj.io/pod-name-format":            {},
}

// noisyObjectFields are bloated internal Kubernetes/Argo fields stripped from sanitized maps.
var noisyObjectFields = map[string]struct{}{
	"managedFields":              {},
	"storedTemplates":            {},
	"storedWorkflowTemplateSpec": {},
}

// SanitizeAnnotations clones an annotation map while removing noisy machine metadata
// and redacting values whose keys look sensitive.
func SanitizeAnnotations(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if _, skip := noisyAnnotations[k]; skip {
			continue
		}
		if IsSensitiveKey(k) {
			out[k] = redactedPlaceholder
		} else {
			out[k] = RedactSecretsInText(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// IsSensitiveKey reports whether a parameter, header, or annotation key name likely holds a secret.
func IsSensitiveKey(key string) bool {
	return sensitiveKeyPattern.MatchString(strings.TrimSpace(key))
}

// RedactParameterValue masks values of sensitive parameter names and redacts inline
// credential patterns in regular parameter values.
func RedactParameterValue(name, value string) string {
	if value == "" {
		return ""
	}
	if IsSensitiveKey(name) {
		return redactedPlaceholder
	}
	return RedactSecretsInText(value)
}

// RedactSecretsInText scrubs bearer tokens, basic auth strings, PEM private keys,
// and common inline secret assignments from untrusted log or message strings.
func RedactSecretsInText(text string) string {
	if text == "" {
		return ""
	}
	out := pemBlockPattern.ReplaceAllString(text, redactedPlaceholder)
	out = bearerTokenPattern.ReplaceAllString(out, "${1}"+redactedPlaceholder)
	out = basicAuthPattern.ReplaceAllString(out, "${1}"+redactedPlaceholder)
	out = inlineSecretPattern.ReplaceAllString(out, "${1}="+redactedPlaceholder)
	return out
}

// SanitizeMap recursively strips Kubernetes/Argo machine noise (managedFields,
// last-applied-configuration, storedTemplates) and redacts Secret objects and sensitive keys.
func SanitizeMap(v any) any {
	switch node := v.(type) {
	case map[string]any:
		// Fallback mask for any raw Kubernetes Secret object.
		if kind, _ := node["kind"].(string); strings.EqualFold(kind, "Secret") {
			return sanitizeSecretObject(node)
		}
		out := make(map[string]any, len(node))
		for k, val := range node {
			if _, drop := noisyObjectFields[k]; drop {
				continue
			}
			if k == "annotations" {
				if annMap, ok := val.(map[string]any); ok {
					cleaned := sanitizeAnnotationMapAny(annMap)
					if len(cleaned) > 0 {
						out[k] = cleaned
					}
					continue
				}
			}
			if IsSensitiveKey(k) {
				if _, isStr := val.(string); isStr {
					out[k] = redactedPlaceholder
					continue
				}
			}
			out[k] = SanitizeMap(val)
		}
		return out
	case []any:
		out := make([]any, len(node))
		for i, elem := range node {
			out[i] = SanitizeMap(elem)
		}
		return out
	case string:
		return RedactSecretsInText(node)
	default:
		return v
	}
}

func sanitizeSecretObject(node map[string]any) map[string]any {
	out := make(map[string]any, len(node))
	for k, val := range node {
		if _, drop := noisyObjectFields[k]; drop {
			continue
		}
		if k == "data" || k == "stringData" {
			if m, ok := val.(map[string]any); ok {
				masked := make(map[string]any, len(m))
				for sk := range m {
					masked[sk] = redactedPlaceholder
				}
				out[k] = masked
				continue
			}
		}
		out[k] = SanitizeMap(val)
	}
	return out
}

func sanitizeAnnotationMapAny(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, val := range in {
		if _, skip := noisyAnnotations[k]; skip {
			continue
		}
		if IsSensitiveKey(k) {
			out[k] = redactedPlaceholder
			continue
		}
		out[k] = SanitizeMap(val)
	}
	return out
}
