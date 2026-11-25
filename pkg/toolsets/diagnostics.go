package toolsets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/clusterworkflowtemplate"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/cronworkflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowtemplate"
	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"

	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/compat"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/curation"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

// =============================================================================
// 8. get_workflow_node (diagnostics)
// =============================================================================

type getWorkflowNodeInput struct {
	TargetInput
	Namespace    string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	WorkflowName string `json:"workflowName" jsonschema:"Workflow name,required"`
	Node         string `json:"node" jsonschema:"Node ID, full node name, displayName, or PodName v2,required"`
}

type getWorkflowNodeOutput struct {
	WorkflowName      string                           `json:"workflowName"`
	Namespace         string                           `json:"namespace"`
	Node              curation.AbnormalNodeDetail      `json:"node"`
	Outputs           []curation.ParameterSummary      `json:"outputParameters,omitempty"`
	OutputArtifacts   []curation.ArtifactSummary       `json:"outputArtifacts,omitempty"`
	OutputResult      string                           `json:"outputResult,omitempty"`
	HostNodeName      string                           `json:"hostNodeName,omitempty"`
	PodIP             string                           `json:"podIp,omitempty"`
	BoundaryID        string                           `json:"boundaryId,omitempty"`
	Children          []string                         `json:"children,omitempty"`
	ContainerStatuses []curation.ContainerStateSummary `json:"containerStatuses,omitempty"`
}

func registerGetWorkflowNode(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "get_workflow_node",
		Description: "Inspect detailed inputs, outputs, artifacts, exit code, computed PodName v2, and container states for a specific workflow node.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetDiagnostics, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowNodeInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if err := argo.ValidatePathSegment(in.WorkflowName, "workflowName"); err != nil {
			return nil, nil, err
		}
		if strings.TrimSpace(in.Node) == "" {
			return nil, nil, fmt.Errorf("node selector cannot be empty")
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}

		wf, _, err := curation.FetchWorkflowTransparent(ctx, client, ns, strings.TrimSpace(in.WorkflowName), "")
		if err != nil {
			return nil, nil, argo.FormatAgentError("get_workflow_node", err)
		}
		node, err := compat.FindNode(wf, in.Node)
		if err != nil {
			return nil, nil, err
		}

		detail := curation.BuildAbnormalNodeDetail(wf, node)
		out := &getWorkflowNodeOutput{
			WorkflowName: wf.Name,
			Namespace:    wf.Namespace,
			Node:         detail,
			HostNodeName: node.HostNodeName,
			PodIP:        node.PodIP,
			BoundaryID:   node.BoundaryID,
			Children:     node.Children,
		}
		if node.Outputs != nil {
			out.Outputs = curation.ExtractParameters(node.Outputs.Parameters)
			out.OutputArtifacts = curation.ExtractArtifacts(node.Outputs.Artifacts)
			if node.Outputs.Result != nil {
				out.OutputResult = governance.GuardUntrustedSnippet(*node.Outputs.Result, 500)
			}
		}
		if detail.PodName != "" && !client.IsArgoServerMode() {
			if pods, pErr := client.GetPodStatuses(ctx, wf.Namespace, []string{detail.PodName}); pErr == nil && pods[detail.PodName] != nil {
				out.ContainerStatuses = curation.ExtractPodContainerStatuses(pods[detail.PodName])
			}
		}

		meta.Namespace = wf.Namespace
		meta.Resource = wf.Name + "/" + node.ID
		return out, nil, nil
	})
}

// =============================================================================
// 9. get_workflow_logs (diagnostics)
// =============================================================================

type getWorkflowLogsInput struct {
	TargetInput
	Namespace string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name      string `json:"name" jsonschema:"Workflow name,required"`
	Node      string `json:"node,omitempty" jsonschema:"Node ID, displayName, or PodName (recommended; automatically resolves Argo v3.6+ PodName v2)"`
	Container string `json:"container,omitempty" jsonschema:"Container name: auto (default; probes main/init/wait intelligently), main, init, or wait"`
	TailLines int64  `json:"tailLines,omitempty" jsonschema:"Number of tail lines to return (default: 100, max: 500)"`
	Grep      string `json:"grep,omitempty" jsonschema:"Filter log lines containing this string"`
}

type getWorkflowLogsOutput struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	PodName   string `json:"podName,omitempty"`
	Container string `json:"container"`
	Truncated bool   `json:"truncated,omitempty"`
	Logs      string `json:"logs"`
}

func registerGetWorkflowLogs(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "get_workflow_logs",
		Description: "Fetch sanitized, prompt-injection-isolated logs from a workflow or specific node (auto-resolves Argo v3.6+ PodName v2 and falls back across init/main/wait containers when container=\"auto\"). Hard-capped at 32KB.",
		Annotations: readOnlyAnnotations(),
	}
	hint := "Pass a specific node ID/displayName, smaller tailLines (e.g. 50), or grep filter."
	registerGovernedTool(s, deps, ToolsetDiagnostics, tool, hint, func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowLogsInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
			return nil, nil, err
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}

		tailLines := in.TailLines
		if tailLines <= 0 {
			tailLines = 100
		}
		if tailLines > 500 {
			tailLines = 500
		}

		container := strings.TrimSpace(in.Container)
		if container == "" {
			container = "auto"
		}

		wfName := strings.TrimSpace(in.Name)
		meta.Resource = wfName

		// If node selector is supplied, resolve exact PodName v2 via the workflow object.
		if nodeSel := strings.TrimSpace(in.Node); nodeSel != "" {
			wf, _, wfErr := curation.FetchWorkflowTransparent(ctx, client, ns, wfName, "")
			if wfErr == nil && wf != nil {
				if node, findErr := compat.FindNode(wf, nodeSel); findErr == nil {
					pod, usedContainer, wrapped, truncated, logErr := curation.FetchSmartNodeLogs(
						ctx, client, wf, node, container, tailLines, in.Grep, governance.DefaultMaxLogBytes,
					)
					if logErr != nil {
						return nil, nil, argo.FormatAgentError("get_workflow_logs", logErr)
					}
					if wrapped == "" {
						wrapped = fmt.Sprintf("<untrusted_workflow_log pod=%q container=%q>\n(no log lines returned)\n</untrusted_workflow_log>", pod, usedContainer)
					}
					return &getWorkflowLogsOutput{
						Name:      wfName,
						Namespace: ns,
						PodName:   pod,
						Container: usedContainer,
						Truncated: truncated,
						Logs:      wrapped,
					}, nil, nil
				}
			}
			// Fallback: caller passed a literal Pod name directly.
			if err := argo.ValidatePathSegment(nodeSel, "node"); err != nil {
				return nil, nil, err
			}
			return fetchDirectWorkflowLogs(ctx, client, ns, wfName, nodeSel, container, tailLines, in.Grep)
		}

		return fetchDirectWorkflowLogs(ctx, client, ns, wfName, "", container, tailLines, in.Grep)
	})
}

func fetchDirectWorkflowLogs(
	ctx context.Context,
	client argo.ClientInterface,
	ns, wfName, podName, container string,
	tailLines int64,
	grep string,
) (*getWorkflowLogsOutput, *mcp.CallToolResult, error) {
	if strings.EqualFold(container, "auto") {
		container = compat.ContainerMain
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.WorkflowService().WorkflowLogs(streamCtx, &workflow.WorkflowLogRequest{
		Namespace: ns,
		Name:      wfName,
		PodName:   podName,
		Grep:      grep,
		LogOptions: &corev1.PodLogOptions{
			Container: container,
			TailLines: &tailLines,
		},
	})
	if err != nil {
		return nil, nil, argo.FormatAgentError("get_workflow_logs", err)
	}

	var sb strings.Builder
	const readBudget = governance.DefaultMaxLogBytes * 2
	for {
		entry, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if sb.Len() > 0 {
				break
			}
			return nil, nil, argo.FormatAgentError("get_workflow_logs", recvErr)
		}
		if entry == nil {
			continue
		}
		if podName == "" && entry.PodName != "" {
			sb.WriteString("[")
			sb.WriteString(entry.PodName)
			sb.WriteString("] ")
		}
		sb.WriteString(entry.Content)
		sb.WriteByte('\n')
		if sb.Len() >= readBudget {
			break
		}
	}

	labelPod := podName
	if labelPod == "" {
		labelPod = wfName + "/*"
	}
	guarded, truncated := governance.CleanAndGuardLogs(sb.String(), labelPod, container, governance.DefaultMaxLogBytes)
	return &getWorkflowLogsOutput{
		Name:      wfName,
		Namespace: ns,
		PodName:   podName,
		Container: container,
		Truncated: truncated,
		Logs:      guarded,
	}, nil, nil
}

// =============================================================================
// 10. get_workflow_events (diagnostics)
// =============================================================================

type getWorkflowEventsInput struct {
	TargetInput
	Namespace    string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name         string `json:"name" jsonschema:"Workflow name,required"`
	OnlyWarnings *bool  `json:"onlyWarnings,omitempty" jsonschema:"Return only Warning events (default: true)"`
	Limit        int    `json:"limit,omitempty" jsonschema:"Maximum events to return (default: 40, max: 100)"`
}

type getWorkflowEventsOutput struct {
	Name      string                  `json:"name"`
	Namespace string                  `json:"namespace"`
	Total     int                     `json:"total"`
	Events    []curation.EventSummary `json:"events"`
}

func registerGetWorkflowEvents(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "get_workflow_events",
		Description: "Fetch Kubernetes events associated with a Workflow and its Pods (FailedScheduling, ImagePullBackOff, FailedMount, OOMKilled). Works symmetrically in both Argo Server mode (via bounded WatchEvents snapshot) and Direct Kubernetes mode.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetDiagnostics, tool, "Pass onlyWarnings=true or lower limit.", func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowEventsInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
			return nil, nil, err
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		wfName := strings.TrimSpace(in.Name)
		meta.Resource = wfName

		onlyWarnings := true
		if in.OnlyWarnings != nil {
			onlyWarnings = *in.OnlyWarnings
		}

		// Optionally inspect workflow nodes to gather exact PodName v2 identifiers.
		var podNames []string
		if wf, _, wfErr := curation.FetchWorkflowTransparent(ctx, client, ns, wfName, ""); wfErr == nil && wf != nil {
			for _, n := range wf.Status.Nodes {
				nc := n
				if nc.Type == wfv1.NodeTypePod || nc.Type == wfv1.NodeTypeContainer {
					if p := compat.ResolveNodePodName(wf, &nc); p != "" {
						podNames = append(podNames, p)
					}
				}
			}
		}

		events, err := client.ListWorkflowEvents(ctx, ns, wfName, podNames, onlyWarnings)
		if err != nil {
			return nil, nil, argo.FormatAgentError("get_workflow_events", err)
		}
		limit := clampLimit(in.Limit, 40, maxListLimit)
		summarized := curation.SummarizeEvents(events, limit)
		if summarized == nil {
			summarized = []curation.EventSummary{}
		}
		return &getWorkflowEventsOutput{
			Name:      wfName,
			Namespace: ns,
			Total:     len(summarized),
			Events:    summarized,
		}, nil, nil
	})
}

// =============================================================================
// 11. render_workflow_graph (diagnostics)
// =============================================================================

type renderWorkflowGraphInput struct {
	TargetInput
	Namespace         string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (when rendering an existing workflow)"`
	Name              string `json:"name,omitempty" jsonschema:"Existing Workflow name (provide name/uid OR manifest)"`
	UID               string `json:"uid,omitempty" jsonschema:"Existing Workflow UID (for archived workflows)"`
	Manifest          string `json:"manifest,omitempty" jsonschema:"Unsubmitted Workflow/WorkflowTemplate/CronWorkflow YAML manifest to preview"`
	Format            string `json:"format,omitempty" jsonschema:"Output format: mermaid (default) or ascii,enum=mermaid,enum=ascii"`
	IncludeStatus     *bool  `json:"includeStatus,omitempty" jsonschema:"Include node execution status colors/symbols (default: true)"`
	CollapseSucceeded *bool  `json:"collapseSucceeded,omitempty" jsonschema:"Collapse succeeded leaf steps into a summary node to keep large DAGs compact (default: auto when >30 nodes)"`
}

func registerRenderWorkflowGraph(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "render_workflow_graph",
		Description: "Render a compact Mermaid or ASCII DAG/Steps graph for either a live/archived Workflow (by name/uid) or an unsubmitted YAML manifest. Supports collapsing succeeded branches on large DAGs.",
		Annotations: readOnlyAnnotations(),
	}
	hint := "Set collapseSucceeded=true or format=\"ascii\" to reduce diagram size."
	registerGovernedTool(s, deps, ToolsetDiagnostics, tool, hint, func(ctx context.Context, _ *mcp.CallToolRequest, in renderWorkflowGraphInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if strings.TrimSpace(in.Manifest) != "" {
			norm, err := compat.NormalizeManifest(in.Manifest)
			if err != nil {
				return nil, nil, err
			}
			var spec *wfv1.WorkflowSpec
			switch norm.Kind {
			case compat.KindWorkflow:
				spec = &norm.Workflow.Spec
			case compat.KindWorkflowTemplate:
				spec = &norm.WorkflowTemplate.Spec
			case compat.KindClusterWorkflowTemplate:
				spec = &norm.ClusterWorkflowTemplate.Spec
			case compat.KindCronWorkflow:
				spec = &norm.CronWorkflow.Spec.WorkflowSpec
			}
			meta.Resource = "manifest:" + norm.Kind
			res, err := curation.RenderSpecGraph(spec, in.Format)
			if err != nil {
				return nil, nil, err
			}
			return res, nil, nil
		}

		if strings.TrimSpace(in.Name) == "" && strings.TrimSpace(in.UID) == "" {
			return nil, nil, fmt.Errorf("provide either manifest OR workflow name/uid")
		}
		if strings.TrimSpace(in.Name) != "" {
			if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
				return nil, nil, err
			}
		}
		if strings.TrimSpace(in.UID) != "" {
			if err := argo.ValidatePathSegment(in.UID, "uid"); err != nil {
				return nil, nil, err
			}
		}

		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		wf, _, err := curation.FetchWorkflowTransparent(ctx, client, ns, strings.TrimSpace(in.Name), strings.TrimSpace(in.UID))
		if err != nil {
			return nil, nil, argo.FormatAgentError("render_workflow_graph", err)
		}
		meta.Namespace = wf.Namespace
		meta.Resource = wf.Name

		includeStatus := true
		if in.IncludeStatus != nil {
			includeStatus = *in.IncludeStatus
		}
		collapseSucceeded := len(wf.Status.Nodes) > 30
		if in.CollapseSucceeded != nil {
			collapseSucceeded = *in.CollapseSucceeded
		}

		res, err := curation.RenderWorkflowGraph(wf, in.Format, includeStatus, collapseSucceeded)
		if err != nil {
			return nil, nil, err
		}
		return res, nil, nil
	})
}

// =============================================================================
// 12. validate_manifest (diagnostics)
// =============================================================================

type validateManifestInput struct {
	TargetInput
	Namespace           string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace for template reference resolution (uses default if omitted)"`
	Manifest            string `json:"manifest" jsonschema:"YAML or JSON manifest (Workflow, WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow),required"`
	OfflineOnly         bool   `json:"offlineOnly,omitempty" jsonschema:"If true, perform local schema parsing and v3.6+/v4 normalization without calling server-side lint"`
	ReturnCanonicalYAML bool   `json:"returnCanonicalYaml,omitempty" jsonschema:"If true, include the normalized v3.6+/v4-compatible YAML in the response"`
}

type validateManifestOutput struct {
	Valid              bool     `json:"valid"`
	Kind               string   `json:"kind"`
	Name               string   `json:"name,omitempty"`
	GenerateName       string   `json:"generateName,omitempty"`
	Namespace          string   `json:"namespace,omitempty"`
	ServerValidated    bool     `json:"serverValidated"`
	NormalizationNotes []string `json:"normalizationNotes,omitempty"`
	Warnings           []string `json:"warnings,omitempty"`
	Errors             []string `json:"errors,omitempty"`
	CanonicalYAML      string   `json:"canonicalYaml,omitempty"`
}

func registerValidateManifest(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "validate_manifest",
		Description: "Unified validator and v3.6+/v4 normalizer for Workflow, WorkflowTemplate, ClusterWorkflowTemplate, and CronWorkflow manifests. Automatically upgrades deprecated singular fields (schedule, mutex, semaphore) and runs server-side lint.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetDiagnostics, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in validateManifestInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		norm, err := compat.NormalizeManifest(in.Manifest)
		if err != nil {
			return &validateManifestOutput{
				Valid:  false,
				Errors: []string{err.Error()},
			}, nil, nil
		}

		out := &validateManifestOutput{
			Valid:              true,
			Kind:               norm.Kind,
			Name:               norm.Name,
			GenerateName:       norm.GenerateName,
			Namespace:          norm.Namespace,
			NormalizationNotes: norm.Changes,
			Warnings:           norm.Warnings,
		}
		if in.ReturnCanonicalYAML {
			out.CanonicalYAML = norm.CanonicalYAML
		}
		meta.Resource = norm.Kind + "/" + norm.Name

		if in.OfflineOnly {
			return out, nil, nil
		}

		isCluster := norm.Kind == compat.KindClusterWorkflowTemplate
		reqNS := strings.TrimSpace(in.Namespace)
		if reqNS == "" {
			reqNS = norm.Namespace
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, reqNS, isCluster, meta)
		if err != nil {
			return nil, nil, err
		}
		if !isCluster {
			out.Namespace = ns
		}

		lintErr := runServerSideLint(ctx, client, norm, ns)
		out.ServerValidated = true
		if lintErr != nil {
			out.Valid = false
			out.Errors = append(out.Errors, lintErr.Error())
		}
		return out, nil, nil
	})
}

func runServerSideLint(ctx context.Context, client argo.ClientInterface, norm *compat.NormalizedManifest, ns string) error {
	switch norm.Kind {
	case compat.KindWorkflow:
		wf := norm.Workflow.DeepCopy()
		wf.Namespace = ns
		_, err := client.WorkflowService().LintWorkflow(ctx, &workflow.WorkflowLintRequest{
			Namespace: ns,
			Workflow:  wf,
		})
		return err
	case compat.KindWorkflowTemplate:
		svc, err := client.WorkflowTemplateService()
		if err != nil {
			return err
		}
		wft := norm.WorkflowTemplate.DeepCopy()
		wft.Namespace = ns
		_, err = svc.LintWorkflowTemplate(ctx, &workflowtemplate.WorkflowTemplateLintRequest{
			Namespace: ns,
			Template:  wft,
		})
		return err
	case compat.KindClusterWorkflowTemplate:
		svc, err := client.ClusterWorkflowTemplateService()
		if err != nil {
			return err
		}
		_, err = svc.LintClusterWorkflowTemplate(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateLintRequest{
			Template: norm.ClusterWorkflowTemplate.DeepCopy(),
		})
		return err
	case compat.KindCronWorkflow:
		svc, err := client.CronWorkflowService()
		if err != nil {
			return err
		}
		cw := norm.CronWorkflow.DeepCopy()
		cw.Namespace = ns
		_, err = svc.LintCronWorkflow(ctx, &cronworkflow.LintCronWorkflowRequest{
			Namespace:    ns,
			CronWorkflow: cw,
		})
		return err
	default:
		return fmt.Errorf("unsupported kind %q", norm.Kind)
	}
}
