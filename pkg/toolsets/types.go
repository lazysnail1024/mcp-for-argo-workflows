// Package toolsets defines modular, intent-driven MCP toolsets (core, diagnostics, ops, admin)
// with unified governance, byte-breaking, scope fencing, SEP-2243 x-mcp-header binding, and auditing.
package toolsets

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/pipekit/mcp-for-argo-workflows/internal/audit"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/compat"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

// ToolsetName identifies a statically gated group of MCP tools.
type ToolsetName string

const (
	ToolsetCore        ToolsetName = "core"
	ToolsetDiagnostics ToolsetName = "diagnostics"
	ToolsetOps         ToolsetName = "ops"
	ToolsetAdmin       ToolsetName = "admin"
)

// AllToolsetNames lists all valid toolset names in logical order.
var AllToolsetNames = []ToolsetName{
	ToolsetCore,
	ToolsetDiagnostics,
	ToolsetOps,
	ToolsetAdmin,
}

// TargetInput is embedded into cluster-facing tool inputs to provide symmetric per-call
// target selection (argoBaseUrl in Argo Server mode; context in Direct K8s mode).
// Incompatible fields are automatically stripped from the advertised JSON Schema.
type TargetInput struct {
	ArgoBaseURL string `json:"argoBaseUrl,omitempty" jsonschema:"Optional Argo Server base URL override for this call (Argo Server mode only; non-default instances must be registered in ARGO_TOKEN_REGISTRY_PATH)"`
	KubeContext string `json:"context,omitempty" jsonschema:"Optional kubeconfig context name for this call (Direct Kubernetes multi-context mode only)"`
}

// Dependencies supplies shared runtime services and governance policies to tool handlers.
type Dependencies struct {
	Client           argo.ClientInterface
	ScopeFence       *governance.ScopeFence
	MRTRGate         *governance.MRTRGate
	MaxResponseBytes int
}

// SelectionConfig controls which toolsets and individual tools are registered.
type SelectionConfig struct {
	Toolsets []string
	Tools    []string
	ReadOnly bool
}

// ToolMeta captures static registration metadata for a single MCP tool.
type ToolMeta struct {
	Name     string
	Toolset  ToolsetName
	ReadOnly bool
	Register func(s *mcp.Server, deps Dependencies)
}

// reservedProtocolFields must never appear in any tool InputSchema (SEP-2322 MRTR invariant).
var reservedProtocolFields = map[string]bool{
	"inputResponses": true,
	"requestState":   true,
}

// routableHeaderBindings attaches non-secret SEP-2243 x-mcp-header annotations to tool schemas.
var routableHeaderBindings = map[string]string{
	"argoBaseUrl":  "Argo-Instance",
	"context":      "Kube-Context",
	"namespace":    "Namespace",
	"name":         "Workflow-Name",
	"workflowName": "Workflow-Name",
	"kind":         "Resource-Kind",
}

// buildToolSchema infers the JSON Schema for In, strips inapplicable target parameters,
// rejects protocol-reserved field names, and annotates routable non-secret properties
// with SEP-2243 x-mcp-header bindings.
func buildToolSchema[In any](toolName string, client argo.ClientInterface) *jsonschema.Schema {
	schema, err := jsonschema.For[In](&jsonschema.ForOptions{})
	if err != nil {
		panic(fmt.Sprintf("tool %q: inferring input schema: %v", toolName, err))
	}

	if !client.IsArgoServerMode() {
		delete(schema.Properties, "argoBaseUrl")
		schema.PropertyOrder = slices.DeleteFunc(schema.PropertyOrder, func(p string) bool { return p == "argoBaseUrl" })
	}
	if !client.MultiContextEnabled() {
		delete(schema.Properties, "context")
		schema.PropertyOrder = slices.DeleteFunc(schema.PropertyOrder, func(p string) bool { return p == "context" })
	}

	seenHeaders := make(map[string]bool)
	for propName, prop := range schema.Properties {
		if reservedProtocolFields[propName] {
			panic(fmt.Sprintf("tool %q: input property %q collides with reserved MCP protocol field", toolName, propName))
		}
		if hdr, ok := routableHeaderBindings[propName]; ok && prop != nil && prop.Type == "string" {
			lower := strings.ToLower(hdr)
			if !seenHeaders[lower] {
				seenHeaders[lower] = true
				if prop.Extra == nil {
					prop.Extra = make(map[string]any, 1)
				}
				prop.Extra["x-mcp-header"] = hdr
			}
		}
	}

	return schema
}

// ExecutionMeta carries context resolved inside a governed tool handler for audit logging.
type ExecutionMeta struct {
	Target    string
	Namespace string
	Resource  string
}

// registerGovernedTool registers a typed tool wrapped with:
//   - SEP-2243 x-mcp-header schema annotation & mode-aware property stripping
//   - traceparent propagation to downstream HTTP/K8s calls
//   - 64KB byte-breaker enforcement (refusing malformed truncated JSON)
//   - secret-free single-line JSON audit logging
func registerGovernedTool[In any](
	s *mcp.Server,
	deps Dependencies,
	toolset ToolsetName,
	tool *mcp.Tool,
	byteLimitHint string,
	fn func(ctx context.Context, req *mcp.CallToolRequest, in In, meta *ExecutionMeta) (any, *mcp.CallToolResult, error),
) {
	tool.InputSchema = buildToolSchema[In](tool.Name, deps.Client)

	handler := func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		start := time.Now()
		caller := audit.ExtractCaller(req)
		traceparent := audit.ExtractTraceparent(req)
		if traceparent != "" {
			ctx = compat.WithTraceparent(ctx, traceparent)
		}

		meta := &ExecutionMeta{
			Target: deps.Client.TargetID(),
		}

		payload, customResult, err := fn(ctx, req, in, meta)
		duration := time.Since(start)

		if err != nil {
			audit.LogToolCall(audit.ToolAuditEntry{
				Caller:       caller,
				Traceparent:  traceparent,
				Tool:         tool.Name,
				Toolset:      string(toolset),
				Target:       meta.Target,
				Namespace:    meta.Namespace,
				Resource:     meta.Resource,
				Duration:     duration,
				IsError:      true,
				ErrorSummary: err.Error(),
			})
			res := &mcp.CallToolResult{}
			res.SetError(err)
			return res, nil, nil
		}

		// MRTR input-required challenges return directly without wrapping in TextContent.
		if customResult != nil && len(customResult.InputRequests) > 0 {
			audit.LogToolCall(audit.ToolAuditEntry{
				Caller:      caller,
				Traceparent: traceparent,
				Tool:        tool.Name,
				Toolset:     string(toolset),
				Target:      meta.Target,
				Namespace:   meta.Namespace,
				Resource:    meta.Resource,
				Duration:    duration,
				IsError:     false,
			})
			return customResult, nil, nil
		}

		rawJSON, limitErr := governance.EnforceResponseLimit(payload, deps.MaxResponseBytes, byteLimitHint)
		if limitErr != nil {
			audit.LogToolCall(audit.ToolAuditEntry{
				Caller:       caller,
				Traceparent:  traceparent,
				Tool:         tool.Name,
				Toolset:      string(toolset),
				Target:       meta.Target,
				Namespace:    meta.Namespace,
				Resource:     meta.Resource,
				Duration:     duration,
				IsError:      true,
				ErrorSummary: limitErr.Error(),
			})
			res := &mcp.CallToolResult{}
			res.SetError(limitErr)
			return res, nil, nil
		}

		audit.LogToolCall(audit.ToolAuditEntry{
			Caller:      caller,
			Traceparent: traceparent,
			Tool:        tool.Name,
			Toolset:     string(toolset),
			Target:      meta.Target,
			Namespace:   meta.Namespace,
			Resource:    meta.Resource,
			Duration:    duration,
			IsError:     false,
		})

		if customResult != nil {
			return customResult, nil, nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: string(rawJSON)},
			},
		}, nil, nil
	}

	mcp.AddTool(s, tool, handler)
}

func resolveClientAndNamespace(
	ctx context.Context,
	deps Dependencies,
	target TargetInput,
	requestedNS string,
	allowAllNamespaces bool,
	meta *ExecutionMeta,
) (context.Context, argo.ClientInterface, string, error) {
	resolvedCtx, client, err := deps.Client.ResolveTarget(ctx, target.ArgoBaseURL, target.KubeContext)
	if err != nil {
		return nil, nil, "", err
	}
	if meta != nil {
		meta.Target = client.TargetID()
	}

	ns := strings.TrimSpace(requestedNS)
	if ns == "" && !allowAllNamespaces {
		ns = client.DefaultNamespace()
	}
	if ns != "" {
		if err := argo.ValidatePathSegment(ns, "namespace"); err != nil {
			return nil, nil, "", err
		}
		if err := deps.ScopeFence.CheckNamespace(ns); err != nil {
			return nil, nil, "", err
		}
	}
	if meta != nil {
		meta.Namespace = ns
	}
	return resolvedCtx, client, ns, nil
}
