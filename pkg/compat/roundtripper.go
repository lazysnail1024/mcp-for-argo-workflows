package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type traceparentContextKey struct{}

// WithTraceparent attaches an inbound W3C traceparent header to ctx so outbound
// Argo Server / Kubernetes HTTP calls propagate distributed tracing context.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	trimmed := strings.TrimSpace(traceparent)
	if trimmed == "" {
		return ctx
	}
	return context.WithValue(ctx, traceparentContextKey{}, trimmed)
}

// TraceparentFromContext returns the W3C traceparent stored in ctx, if any.
func TraceparentFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(traceparentContextKey{}).(string); ok {
		return v
	}
	return ""
}

// NormalizingRoundTripper intercepts JSON HTTP responses from Argo Workflows v3.6/v3.7
// servers or Kubernetes API servers and upgrades deprecated v3 singular fields
// (spec.schedule, synchronization.mutex, synchronization.semaphore) into their v3.6+/v4
// plural equivalents before v4 Go structs decode them. Streaming responses (SSE/logs)
// pass straight through without buffering.
type NormalizingRoundTripper struct {
	Base http.RoundTripper
}

// NewNormalizingRoundTripper constructs a NormalizingRoundTripper wrapping base.
func NewNormalizingRoundTripper(base http.RoundTripper) *NormalizingRoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &NormalizingRoundTripper{Base: base}
}

// WrapRoundTripper matches k8s.io/client-go rest.Config.Wrap signature.
func WrapRoundTripper(base http.RoundTripper) http.RoundTripper {
	return NewNormalizingRoundTripper(base)
}

// RoundTrip executes the HTTP request, propagates traceparent, and normalizes legacy
// Argo v3 JSON payloads on non-streaming 200 OK responses.
func (rt *NormalizingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	base := rt.Base
	if base == nil {
		base = http.DefaultTransport
	}

	outReq := req
	if tp := TraceparentFromContext(req.Context()); tp != "" && req.Header.Get("traceparent") == "" {
		outReq = req.Clone(req.Context())
		outReq.Header.Set("traceparent", tp)
	}

	resp, err := base.RoundTrip(outReq)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}

	if resp.StatusCode != http.StatusOK || !shouldNormalizeResponse(outReq, resp) {
		return resp, nil
	}

	// Read body into memory for lossless v3 -> v4 JSON field repair.
	// Cap normalization scan at 32 MiB so pathological blobs never exhaust memory.
	const maxInspectBytes = 32 << 20
	bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, maxInspectBytes+1))
	_ = resp.Body.Close()
	if readErr != nil {
		resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		return resp, nil
	}
	if len(bodyBytes) > maxInspectBytes {
		resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		return resp, nil
	}

	normalized, changed := NormalizeJSONPayload(bodyBytes)
	if changed {
		resp.Body = io.NopCloser(bytes.NewReader(normalized))
		resp.ContentLength = int64(len(normalized))
		resp.Header.Del("Content-Length")
	} else {
		resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}
	return resp, nil
}

func shouldNormalizeResponse(req *http.Request, resp *http.Response) bool {
	if strings.EqualFold(req.Header.Get("Accept"), "text/event-stream") {
		return false
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(ct, "text/event-stream") {
		return false
	}
	if !strings.Contains(ct, "application/json") {
		return false
	}
	path := req.URL.Path
	// Never buffer watch/stream/log endpoints even if a proxy advertises JSON.
	if strings.Contains(path, "/log") ||
		strings.Contains(path, "/stream/") ||
		strings.Contains(path, "/workflow-events/") ||
		req.URL.Query().Get("watch") == "true" {
		return false
	}
	return true
}

// NormalizeJSONPayload upgrades v3.6/v3.7 singular Argo fields in a JSON payload
// (single object or Kubernetes/Argo list response) to v3.6+/v4 plural fields.
// Returns the modified payload and true if any legacy field was upgraded.
func NormalizeJSONPayload(raw []byte) ([]byte, bool) {
	// Fast-path byte scan avoids JSON decoding overhead when no legacy key appears.
	if !bytes.Contains(raw, []byte(`"schedule"`)) &&
		!bytes.Contains(raw, []byte(`"mutex"`)) &&
		!bytes.Contains(raw, []byte(`"semaphore"`)) {
		return raw, false
	}

	var doc any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		return raw, false
	}

	changed := normalizeObjectTree(doc)
	if !changed {
		return raw, false
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return raw, false
	}
	return out, true
}

func normalizeObjectTree(node any) bool {
	switch v := node.(type) {
	case map[string]any:
		changed := false
		// Handle list envelopes (items: [...]).
		if items, ok := v["items"].([]any); ok {
			for _, item := range items {
				if normalizeObjectTree(item) {
					changed = true
				}
			}
		}
		// Handle Argo/K8s object spec.
		if spec, ok := v["spec"].(map[string]any); ok {
			if normalizeSpecMap(spec) {
				changed = true
			}
		}
		// Handle archived workflow or nested workflow wrapper.
		if wf, ok := v["workflow"].(map[string]any); ok {
			if normalizeObjectTree(wf) {
				changed = true
			}
		}
		return changed
	case []any:
		changed := false
		for _, elem := range v {
			if normalizeObjectTree(elem) {
				changed = true
			}
		}
		return changed
	default:
		return false
	}
}

func normalizeSpecMap(spec map[string]any) bool {
	changed := false

	// 1. CronWorkflowSpec: migrate singular "schedule" -> plural "schedules".
	if rawSched, hasSched := spec["schedule"]; hasSched {
		if schedStr, ok := rawSched.(string); ok && strings.TrimSpace(schedStr) != "" {
			existing, _ := spec["schedules"].([]any)
			if len(existing) == 0 {
				spec["schedules"] = []any{schedStr}
			}
			delete(spec, "schedule")
			changed = true
		}
	}

	// 2. CronWorkflowSpec embeds workflowSpec.
	if wfSpec, ok := spec["workflowSpec"].(map[string]any); ok {
		if normalizeSpecMap(wfSpec) {
			changed = true
		}
	}

	// 3. WorkflowSpec-level synchronization.
	if syncMap, ok := spec["synchronization"].(map[string]any); ok {
		if normalizeSyncMap(syncMap) {
			changed = true
		}
	}

	// 4. Template-level synchronization.
	if templates, ok := spec["templates"].([]any); ok {
		for _, rawTmpl := range templates {
			tmpl, ok := rawTmpl.(map[string]any)
			if !ok {
				continue
			}
			if syncMap, ok := tmpl["synchronization"].(map[string]any); ok {
				if normalizeSyncMap(syncMap) {
					changed = true
				}
			}
		}
	}

	return changed
}

func normalizeSyncMap(syncMap map[string]any) bool {
	changed := false

	if rawMutex, ok := syncMap["mutex"]; ok && rawMutex != nil {
		existing, _ := syncMap["mutexes"].([]any)
		syncMap["mutexes"] = append(existing, rawMutex)
		delete(syncMap, "mutex")
		changed = true
	}

	if rawSem, ok := syncMap["semaphore"]; ok && rawSem != nil {
		existing, _ := syncMap["semaphores"].([]any)
		syncMap["semaphores"] = append(existing, rawSem)
		delete(syncMap, "semaphore")
		changed = true
	}

	return changed
}
