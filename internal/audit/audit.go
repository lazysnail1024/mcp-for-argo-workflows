// Package audit provides structured, secret-free single-line JSON audit logging
// and request-scoped caller/trace metadata extraction.
package audit

import (
	"log/slog"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

const maxCallerLen = 128

// ToolAuditEntry captures sanitized metadata for a single MCP tool execution.
type ToolAuditEntry struct {
	Caller       string
	Traceparent  string
	Tool         string
	Toolset      string
	Target       string
	Namespace    string
	Resource     string
	Duration     time.Duration
	IsError      bool
	ErrorSummary string
}

// ExtractCaller returns the sanitized X-MCP-Caller identifier from the incoming MCP request.
func ExtractCaller(req *mcp.CallToolRequest) string {
	if req == nil || req.Extra == nil || req.Extra.Header == nil {
		return ""
	}
	raw := strings.TrimSpace(req.Extra.Header.Get("X-MCP-Caller"))
	if raw == "" {
		return ""
	}
	cleaned := governance.GuardUntrustedSnippet(raw, maxCallerLen)
	cleaned = strings.ReplaceAll(cleaned, "\n", " ")
	cleaned = strings.ReplaceAll(cleaned, "\r", " ")
	return strings.TrimSpace(cleaned)
}

// ExtractTraceparent returns the W3C traceparent header from MCP _meta or HTTP headers.
func ExtractTraceparent(req *mcp.CallToolRequest) string {
	if req == nil {
		return ""
	}
	if req.Params != nil && req.Params.Meta != nil {
		if tp, ok := req.Params.Meta["traceparent"].(string); ok && strings.TrimSpace(tp) != "" {
			return strings.TrimSpace(tp)
		}
	}
	if req.Extra != nil && req.Extra.Header != nil {
		if tp := strings.TrimSpace(req.Extra.Header.Get("traceparent")); tp != "" {
			return tp
		}
	}
	return ""
}

// LogToolCall writes a single-line structured JSON audit entry to the default slog logger.
// Outbound/inbound tokens are never logged.
func LogToolCall(entry ToolAuditEntry) {
	attrs := []any{
		"tool", entry.Tool,
		"toolset", entry.Toolset,
		"duration_ms", entry.Duration.Milliseconds(),
		"is_error", entry.IsError,
	}
	if entry.Caller != "" {
		attrs = append(attrs, "caller", entry.Caller)
	}
	if entry.Target != "" {
		attrs = append(attrs, "target", entry.Target)
	}
	if entry.Namespace != "" {
		attrs = append(attrs, "namespace", entry.Namespace)
	}
	if entry.Resource != "" {
		attrs = append(attrs, "resource", entry.Resource)
	}
	if entry.Traceparent != "" {
		attrs = append(attrs, "traceparent", entry.Traceparent)
	}
	if entry.IsError && entry.ErrorSummary != "" {
		attrs = append(attrs, "error", governance.GuardUntrustedSnippet(entry.ErrorSummary, 256))
	}
	slog.Info("mcp_tool_call", attrs...)
}
