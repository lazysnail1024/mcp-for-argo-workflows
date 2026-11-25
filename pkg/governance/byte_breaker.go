package governance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	// DefaultMaxResponseBytes enforces a 64KB hard ceiling per tool response to protect
	// LLM context windows and reverse-proxy buffers.
	DefaultMaxResponseBytes = 64 * 1024

	// MinMaxResponseBytes prevents misconfiguration below a usable diagnostic floor.
	MinMaxResponseBytes = 4 * 1024
)

// ByteLimitExceededError is returned instead of emitting truncated/corrupted JSON.
type ByteLimitExceededError struct {
	ActualBytes int
	LimitBytes  int
	Hint        string
}

func (e *ByteLimitExceededError) Error() string {
	hint := e.Hint
	if strings.TrimSpace(hint) == "" {
		hint = "Narrow the query using diagnose_workflow, get_workflow(detailLevel=\"abnormal\"), get_workflow_node, or stricter namespace/labels/limit/grep filters."
	}
	return fmt.Sprintf(
		"response size (%d bytes) exceeded MCP_MAX_RESPONSE_BYTES limit (%d bytes); refusing to return truncated JSON. %s",
		e.ActualBytes, e.LimitBytes, hint,
	)
}

// ResolveMaxResponseBytes reads MCP_MAX_RESPONSE_BYTES with sane defaults and floor.
func ResolveMaxResponseBytes(override int) int {
	if override > 0 {
		if override < MinMaxResponseBytes {
			return MinMaxResponseBytes
		}
		return override
	}
	if raw := strings.TrimSpace(os.Getenv("MCP_MAX_RESPONSE_BYTES")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= MinMaxResponseBytes {
			return n
		}
	}
	return DefaultMaxResponseBytes
}

// EnforceResponseLimit serializes payload to JSON (with HTML escaping disabled so tags like
// <untrusted_workflow_log> and Mermaid arrows --> stay clean and compact) and rejects
// responses exceeding maxBytes with an actionable self-healing error rather than emitting
// malformed truncated JSON.
func EnforceResponseLimit(payload any, maxBytes int, hint string) ([]byte, error) {
	limit := ResolveMaxResponseBytes(maxBytes)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(payload); err != nil {
		return nil, fmt.Errorf("failed to serialize tool response: %w", err)
	}
	data := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	if len(data) > limit {
		return nil, &ByteLimitExceededError{
			ActualBytes: len(data),
			LimitBytes:  limit,
			Hint:        hint,
		}
	}
	return data, nil
}
