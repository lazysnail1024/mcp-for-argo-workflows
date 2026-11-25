package toolsets

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/clusterworkflowtemplate"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/cronworkflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowarchive"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowtemplate"
	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/robfig/cron/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/curation"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

const (
	defaultListLimit = 30
	maxListLimit     = 100
)

func readOnlyAnnotations() *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{
		ReadOnlyHint:    true,
		DestructiveHint: ptr.To(false),
		IdempotentHint:  true,
		OpenWorldHint:   ptr.To(false),
	}
}

// =============================================================================
// 1. list_clusters (core)
// =============================================================================

type listClustersInput struct{}

type listClustersOutput struct {
	Mode    string                  `json:"mode"`
	Targets []argo.TargetDescriptor `json:"targets"`
}

func registerListClusters(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "list_clusters",
		Description: "List configured Argo Workflows targets (Argo Server instances or Kubernetes contexts), active connection mode, probed Argo version (v3.6+/v4.x), and Archive availability.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetCore, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, _ listClustersInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		targets, err := deps.Client.ListTargets(ctx)
		if err != nil {
			return nil, nil, argo.FormatAgentError("list_clusters", err)
		}
		mode := "kubernetes"
		if deps.Client.IsArgoServerMode() {
			mode = "argo-server"
		}
		meta.Resource = fmt.Sprintf("targets:%d", len(targets))
		return &listClustersOutput{
			Mode:    mode,
			Targets: targets,
		}, nil, nil
	})
}

// =============================================================================
// 2. list_workflows (core)
// =============================================================================

type listWorkflowsInput struct {
	TargetInput
	Namespace        *string  `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (defaults to configured namespace; pass empty string \"\" for all allowed namespaces)"`
	Search           string   `json:"search,omitempty" jsonschema:"Case-insensitive substring match against workflow name"`
	Status           []string `json:"status,omitempty" jsonschema:"Filter by phases: Pending, Running, Succeeded, Failed, Error"`
	Labels           string   `json:"labels,omitempty" jsonschema:"Kubernetes label selector (e.g. app=etl,env=prod)"`
	CronWorkflow     string   `json:"cronWorkflow,omitempty" jsonschema:"Filter workflows spawned by a specific CronWorkflow name"`
	WorkflowTemplate string   `json:"workflowTemplate,omitempty" jsonschema:"Filter workflows created from a specific WorkflowTemplate name"`
	Since            string   `json:"since,omitempty" jsonschema:"Only return workflows created within this duration (e.g. 1h, 24h, 7d)"`
	IncludeArchived  *bool    `json:"includeArchived,omitempty" jsonschema:"Include archived workflows when connected via Argo Server (default: true)"`
	Limit            int      `json:"limit,omitempty" jsonschema:"Maximum items to return (default: 30, max: 100)"`
	Offset           int      `json:"offset,omitempty" jsonschema:"Pagination offset (default: 0)"`
}

type workflowListItem struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	UID        string `json:"uid,omitempty"`
	Source     string `json:"source"`
	Phase      string `json:"phase"`
	Progress   string `json:"progress,omitempty"`
	CreatedAt  string `json:"createdAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
	Duration   string `json:"duration,omitempty"`
	Message    string `json:"message,omitempty"`
}

type listWorkflowsOutput struct {
	Namespace    string             `json:"namespace"`
	TotalMatched int                `json:"totalMatched"`
	Offset       int                `json:"offset"`
	Limit        int                `json:"limit"`
	Workflows    []workflowListItem `json:"workflows"`
}

func registerListWorkflows(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "list_workflows",
		Description: "List Argo Workflows (transparently merging live and archived runs in Argo Server mode) with search, phase, owner template/cron, time window, and pagination filters.",
		Annotations: readOnlyAnnotations(),
	}
	hint := "Reduce limit (e.g. limit=20) or filter by namespace, status, search, cronWorkflow, or since."
	registerGovernedTool(s, deps, ToolsetCore, tool, hint, func(ctx context.Context, _ *mcp.CallToolRequest, in listWorkflowsInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		allowAll := in.Namespace != nil && strings.TrimSpace(*in.Namespace) == ""
		reqNS := ""
		if in.Namespace != nil {
			reqNS = *in.Namespace
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, reqNS, allowAll, meta)
		if err != nil {
			return nil, nil, err
		}

		phaseFilter, err := parsePhaseFilter(in.Status)
		if err != nil {
			return nil, nil, err
		}
		var sinceCutoff time.Time
		if strings.TrimSpace(in.Since) != "" {
			dur, dErr := parseFlexibleDuration(in.Since)
			if dErr != nil {
				return nil, nil, fmt.Errorf("invalid since duration %q: %w", in.Since, dErr)
			}
			sinceCutoff = time.Now().Add(-dur)
		}

		labelParts := make([]string, 0, 3)
		if strings.TrimSpace(in.Labels) != "" {
			labelParts = append(labelParts, strings.TrimSpace(in.Labels))
		}
		if strings.TrimSpace(in.CronWorkflow) != "" {
			if err := argo.ValidatePathSegment(in.CronWorkflow, "cronWorkflow"); err != nil {
				return nil, nil, err
			}
			labelParts = append(labelParts, "workflows.argoproj.io/cron-workflow="+strings.TrimSpace(in.CronWorkflow))
		}
		if strings.TrimSpace(in.WorkflowTemplate) != "" {
			if err := argo.ValidatePathSegment(in.WorkflowTemplate, "workflowTemplate"); err != nil {
				return nil, nil, err
			}
			labelParts = append(labelParts, "workflows.argoproj.io/workflow-template="+strings.TrimSpace(in.WorkflowTemplate))
		}
		labelSelector := strings.Join(labelParts, ",")

		wfSvc := client.WorkflowService()
		liveResp, err := wfSvc.ListWorkflows(ctx, &workflow.WorkflowListRequest{
			Namespace: ns,
			ListOptions: &metav1.ListOptions{
				LabelSelector: labelSelector,
			},
			Fields: "items.metadata,items.status.phase,items.status.message,items.status.startedAt,items.status.finishedAt,items.status.progress",
		})
		if err != nil {
			return nil, nil, argo.FormatAgentError("list_workflows", err)
		}

		seenUID := make(map[string]bool)
		type entry struct {
			item      workflowListItem
			createdAt time.Time
		}
		collected := make([]entry, 0)

		searchLower := strings.ToLower(strings.TrimSpace(in.Search))
		appendWf := func(wf *wfv1.Workflow, source string) {
			if wf == nil || !deps.ScopeFence.IsNamespaceAllowed(wf.Namespace) {
				return
			}
			uid := string(wf.UID)
			if uid != "" {
				if seenUID[uid] {
					return
				}
				seenUID[uid] = true
			}
			phase := curation.NormalizeWorkflowPhase(string(wf.Status.Phase))
			if len(phaseFilter) > 0 && !phaseFilter[phase] {
				return
			}
			if searchLower != "" && !strings.Contains(strings.ToLower(wf.Name), searchLower) {
				return
			}
			created := wf.CreationTimestamp.Time
			if !sinceCutoff.IsZero() && !created.IsZero() && created.Before(sinceCutoff) {
				return
			}

			item := workflowListItem{
				Name:      wf.Name,
				Namespace: wf.Namespace,
				UID:       uid,
				Source:    source,
				Phase:     phase,
				Progress:  string(wf.Status.Progress),
				Message:   governance.GuardUntrustedSnippet(wf.Status.Message, 200),
			}
			if !created.IsZero() {
				item.CreatedAt = created.Format(time.RFC3339)
			}
			if !wf.Status.FinishedAt.Time.IsZero() {
				item.FinishedAt = wf.Status.FinishedAt.Format(time.RFC3339)
			}
			if !wf.Status.StartedAt.Time.IsZero() {
				end := wf.Status.FinishedAt.Time
				if end.IsZero() {
					end = time.Now()
				}
				item.Duration = curation.FormatDuration(end.Sub(wf.Status.StartedAt.Time))
			}
			collected = append(collected, entry{item: item, createdAt: created})
		}

		if liveResp != nil {
			for i := range liveResp.Items {
				appendWf(&liveResp.Items[i], "live")
			}
		}

		includeArchived := true
		if in.IncludeArchived != nil {
			includeArchived = *in.IncludeArchived
		}
		if includeArchived && client.IsArgoServerMode() {
			if archSvc, archErr := client.ArchivedWorkflowService(); archErr == nil {
				archOpts := &metav1.ListOptions{
					LabelSelector: labelSelector,
					Limit:         200,
				}
				if ns != "" {
					archOpts.FieldSelector = "metadata.namespace=" + ns
				}
				if archResp, listErr := archSvc.ListArchivedWorkflows(ctx, &workflowarchive.ListArchivedWorkflowsRequest{
					ListOptions: archOpts,
				}); listErr == nil && archResp != nil {
					for i := range archResp.Items {
						appendWf(&archResp.Items[i], "archived")
					}
				}
			}
		}

		// Sort newest first deterministically.
		sort.Slice(collected, func(i, j int) bool {
			if !collected[i].createdAt.Equal(collected[j].createdAt) {
				return collected[i].createdAt.After(collected[j].createdAt)
			}
			return collected[i].item.Name < collected[j].item.Name
		})

		limit := clampLimit(in.Limit, defaultListLimit, maxListLimit)
		offset := max(in.Offset, 0)
		total := len(collected)

		paged := make([]workflowListItem, 0, min(limit, max(total-offset, 0)))
		for i := offset; i < total && len(paged) < limit; i++ {
			paged = append(paged, collected[i].item)
		}

		displayNS := ns
		if displayNS == "" {
			displayNS = "*"
		}
		meta.Resource = fmt.Sprintf("count:%d/%d", len(paged), total)
		return &listWorkflowsOutput{
			Namespace:    displayNS,
			TotalMatched: total,
			Offset:       offset,
			Limit:        limit,
			Workflows:    paged,
		}, nil, nil
	})
}

// =============================================================================
// 3. get_workflow (core)
// =============================================================================

type getWorkflowInput struct {
	TargetInput
	Namespace   string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name        string `json:"name,omitempty" jsonschema:"Workflow name (provide name or uid)"`
	UID         string `json:"uid,omitempty" jsonschema:"Workflow UID (useful for archived workflows)"`
	DetailLevel string `json:"detailLevel,omitempty" jsonschema:"Node detail level: summary (counts only), abnormal (default; strips Succeeded/Skipped/Omitted nodes), or detailed (all nodes),enum=summary,enum=abnormal,enum=detailed"`
}

type getWorkflowOutput struct {
	Name          string                        `json:"name"`
	Namespace     string                        `json:"namespace"`
	UID           string                        `json:"uid,omitempty"`
	Source        string                        `json:"source"`
	Phase         string                        `json:"phase"`
	Message       string                        `json:"message,omitempty"`
	Progress      string                        `json:"progress,omitempty"`
	StartedAt     string                        `json:"startedAt,omitempty"`
	FinishedAt    string                        `json:"finishedAt,omitempty"`
	Duration      string                        `json:"duration,omitempty"`
	Labels        map[string]string             `json:"labels,omitempty"`
	Annotations   map[string]string             `json:"annotations,omitempty"`
	Parameters    []curation.ParameterSummary   `json:"parameters,omitempty"`
	NodeSummary   *curation.NodeSummary         `json:"nodeSummary,omitempty"`
	AbnormalNodes []curation.AbnormalNodeDetail `json:"abnormalNodes,omitempty"`
	AllNodes      []curation.AbnormalNodeDetail `json:"allNodes,omitempty"`
}

func registerGetWorkflow(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "get_workflow",
		Description: "Inspect an Argo Workflow by name or UID (auto-falls back to Archive and decompresses compressedNodes). Defaults to detailLevel=\"abnormal\" to strip succeeded-node noise on large DAGs.",
		Annotations: readOnlyAnnotations(),
	}
	hint := "Use detailLevel=\"summary\" or \"abnormal\", or call diagnose_workflow for curated root-cause analysis."
	registerGovernedTool(s, deps, ToolsetCore, tool, hint, func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		if strings.TrimSpace(in.Name) == "" && strings.TrimSpace(in.UID) == "" {
			return nil, nil, fmt.Errorf("either name or uid is required")
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

		detail := strings.ToLower(strings.TrimSpace(in.DetailLevel))
		if detail == "" {
			detail = "abnormal"
		}
		if detail != "summary" && detail != "abnormal" && detail != "detailed" {
			return nil, nil, fmt.Errorf("invalid detailLevel %q: must be summary, abnormal, or detailed", in.DetailLevel)
		}

		wf, source, err := curation.FetchWorkflowTransparent(ctx, client, ns, strings.TrimSpace(in.Name), strings.TrimSpace(in.UID))
		if err != nil {
			return nil, nil, argo.FormatAgentError("get_workflow", err)
		}
		if err := deps.ScopeFence.CheckNamespace(wf.Namespace); err != nil {
			return nil, nil, err
		}
		meta.Namespace = wf.Namespace
		meta.Resource = wf.Name

		out := &getWorkflowOutput{
			Name:        wf.Name,
			Namespace:   wf.Namespace,
			UID:         string(wf.UID),
			Source:      source,
			Phase:       curation.NormalizeWorkflowPhase(string(wf.Status.Phase)),
			Message:     governance.GuardUntrustedSnippet(wf.Status.Message, 500),
			Progress:    string(wf.Status.Progress),
			Labels:      wf.Labels,
			Annotations: governance.SanitizeAnnotations(wf.Annotations),
			Parameters:  curation.ExtractParameters(wf.Spec.Arguments.Parameters),
			NodeSummary: curation.BuildNodeSummary(wf.Status.Nodes),
		}
		if !wf.Status.StartedAt.Time.IsZero() {
			out.StartedAt = wf.Status.StartedAt.Format(time.RFC3339)
			end := wf.Status.FinishedAt.Time
			if end.IsZero() {
				end = time.Now()
			}
			out.Duration = curation.FormatDuration(end.Sub(wf.Status.StartedAt.Time))
		}
		if !wf.Status.FinishedAt.Time.IsZero() {
			out.FinishedAt = wf.Status.FinishedAt.Format(time.RFC3339)
		}

		switch detail {
		case "abnormal":
			out.AbnormalNodes = curation.ExtractAbnormalNodes(wf)
		case "detailed":
			nodes := make([]curation.AbnormalNodeDetail, 0, len(wf.Status.Nodes))
			for _, n := range wf.Status.Nodes {
				nc := n
				nodes = append(nodes, curation.BuildAbnormalNodeDetail(wf, &nc))
			}
			sort.Slice(nodes, func(i, j int) bool {
				if nodes[i].StartedAt != nodes[j].StartedAt {
					return nodes[i].StartedAt < nodes[j].StartedAt
				}
				return nodes[i].ID < nodes[j].ID
			})
			out.AllNodes = nodes
		}

		return out, nil, nil
	})
}

// =============================================================================
// 4. diagnose_workflow (core)
// =============================================================================

type diagnoseWorkflowInput struct {
	TargetInput
	Namespace string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name      string `json:"name,omitempty" jsonschema:"Workflow name (provide name or uid)"`
	UID       string `json:"uid,omitempty" jsonschema:"Workflow UID (useful for archived workflows)"`
	TailLines int64  `json:"tailLines,omitempty" jsonschema:"Log tail lines per root-cause node (default: 40, max: 200)"`
}

func registerDiagnoseWorkflow(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "diagnose_workflow",
		Description: "High-signal SRE diagnostic tool: concurrently analyzes Workflow status, isolates true root-cause failed leaf nodes from cascading DAG failures, detects Suspend approval pauses, collects Kubernetes Warning events (e.g. FailedScheduling/ImagePullBackOff/OOMKilled), and fetches smart init/main/wait tail logs. Prefer this tool first when troubleshooting any failed, errored, or stalled workflow.",
		Annotations: readOnlyAnnotations(),
	}
	hint := "Reduce tailLines (e.g. tailLines=20) or inspect individual nodes with get_workflow_node and get_workflow_logs."
	registerGovernedTool(s, deps, ToolsetCore, tool, hint, func(ctx context.Context, _ *mcp.CallToolRequest, in diagnoseWorkflowInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		if strings.TrimSpace(in.Name) == "" && strings.TrimSpace(in.UID) == "" {
			return nil, nil, fmt.Errorf("either name or uid is required")
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

		diag, err := curation.DiagnoseWorkflow(ctx, client, ns, strings.TrimSpace(in.Name), strings.TrimSpace(in.UID), in.TailLines)
		if err != nil {
			return nil, nil, argo.FormatAgentError("diagnose_workflow", err)
		}
		if err := deps.ScopeFence.CheckNamespace(diag.Namespace); err != nil {
			return nil, nil, err
		}
		meta.Namespace = diag.Namespace
		meta.Resource = diag.Name
		return diag, nil, nil
	})
}

// =============================================================================
// 5. list_templates (core)
// =============================================================================

type listTemplatesInput struct {
	TargetInput
	Namespace string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace for namespaced WorkflowTemplates (uses default if omitted)"`
	Scope     string `json:"scope,omitempty" jsonschema:"Template scope: namespaced, cluster, or all (default: all),enum=namespaced,enum=cluster,enum=all"`
	Search    string `json:"search,omitempty" jsonschema:"Case-insensitive substring match against template name or description"`
	Labels    string `json:"labels,omitempty" jsonschema:"Kubernetes label selector"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum items to return (default: 50, max: 100)"`
}

type templateListItem struct {
	Name        string   `json:"name"`
	Namespace   string   `json:"namespace,omitempty"`
	ClusterWide bool     `json:"clusterScope"`
	Description string   `json:"description,omitempty"`
	Entrypoint  string   `json:"entrypoint,omitempty"`
	Parameters  []string `json:"parameters,omitempty"`
	CreatedAt   string   `json:"createdAt,omitempty"`
}

type listTemplatesOutput struct {
	Total     int                `json:"total"`
	Templates []templateListItem `json:"templates"`
}

func registerListTemplates(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "list_templates",
		Description: "List WorkflowTemplates and/or ClusterWorkflowTemplates with search filtering and parameter signatures. Use this to discover approved templates before calling trigger_workflow.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetCore, tool, "Filter by scope, namespace, labels, or search keyword.", func(ctx context.Context, _ *mcp.CallToolRequest, in listTemplatesInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}

		scope := strings.ToLower(strings.TrimSpace(in.Scope))
		if scope == "" {
			scope = "all"
		}
		if scope != "namespaced" && scope != "cluster" && scope != "all" {
			return nil, nil, fmt.Errorf("invalid scope %q: must be namespaced, cluster, or all", in.Scope)
		}

		searchLower := strings.ToLower(strings.TrimSpace(in.Search))
		items := make([]templateListItem, 0)

		if scope == "namespaced" || scope == "all" {
			wftSvc, svcErr := client.WorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("list_templates", svcErr)
			}
			resp, listErr := wftSvc.ListWorkflowTemplates(ctx, &workflowtemplate.WorkflowTemplateListRequest{
				Namespace:   ns,
				ListOptions: &metav1.ListOptions{LabelSelector: strings.TrimSpace(in.Labels)},
			})
			if listErr != nil {
				return nil, nil, argo.FormatAgentError("list_templates", listErr)
			}
			if resp != nil {
				for _, wft := range resp.Items {
					if !deps.ScopeFence.IsNamespaceAllowed(wft.Namespace) {
						continue
					}
					item := buildTemplateListItem(wft.Name, wft.Namespace, false, wft.Annotations, wft.CreationTimestamp, &wft.Spec)
					if matchesTemplateSearch(item, searchLower) {
						items = append(items, item)
					}
				}
			}
		}

		if scope == "cluster" || scope == "all" {
			cwftSvc, svcErr := client.ClusterWorkflowTemplateService()
			if svcErr == nil {
				resp, listErr := cwftSvc.ListClusterWorkflowTemplates(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateListRequest{
					ListOptions: &metav1.ListOptions{LabelSelector: strings.TrimSpace(in.Labels)},
				})
				if listErr != nil {
					if scope == "cluster" {
						return nil, nil, argo.FormatAgentError("list_templates", listErr)
					}
				} else if resp != nil {
					for _, cwft := range resp.Items {
						item := buildTemplateListItem(cwft.Name, "", true, cwft.Annotations, cwft.CreationTimestamp, &cwft.Spec)
						if matchesTemplateSearch(item, searchLower) {
							items = append(items, item)
						}
					}
				}
			} else if scope == "cluster" {
				return nil, nil, argo.FormatAgentError("list_templates", svcErr)
			}
		}

		sort.Slice(items, func(i, j int) bool {
			if items[i].ClusterWide != items[j].ClusterWide {
				return !items[i].ClusterWide
			}
			return items[i].Name < items[j].Name
		})

		limit := clampLimit(in.Limit, 50, maxListLimit)
		total := len(items)
		if len(items) > limit {
			items = items[:limit]
		}
		meta.Resource = fmt.Sprintf("templates:%d", len(items))
		return &listTemplatesOutput{
			Total:     total,
			Templates: items,
		}, nil, nil
	})
}

func buildTemplateListItem(name, ns string, clusterWide bool, ann map[string]string, created metav1.Time, spec *wfv1.WorkflowSpec) templateListItem {
	item := templateListItem{
		Name:        name,
		Namespace:   ns,
		ClusterWide: clusterWide,
		Entrypoint:  spec.Entrypoint,
	}
	if ann != nil {
		item.Description = governance.GuardUntrustedSnippet(ann["workflows.argoproj.io/description"], 180)
	}
	if !created.IsZero() {
		item.CreatedAt = created.Format(time.RFC3339)
	}
	for _, p := range spec.Arguments.Parameters {
		item.Parameters = append(item.Parameters, p.Name)
	}
	return item
}

func matchesTemplateSearch(item templateListItem, searchLower string) bool {
	if searchLower == "" {
		return true
	}
	return strings.Contains(strings.ToLower(item.Name), searchLower) ||
		strings.Contains(strings.ToLower(item.Description), searchLower)
}

// =============================================================================
// 6. get_template (core)
// =============================================================================

type getTemplateInput struct {
	TargetInput
	Name         string `json:"name" jsonschema:"WorkflowTemplate or ClusterWorkflowTemplate name,required"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (ignored when clusterScope=true)"`
	ClusterScope bool   `json:"clusterScope,omitempty" jsonschema:"Set true to fetch a ClusterWorkflowTemplate instead of a namespaced WorkflowTemplate"`
}

type templateParameterSpec struct {
	Name        string   `json:"name"`
	Default     string   `json:"default,omitempty"`
	Description string   `json:"description,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

type templateStepSummary struct {
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	Image        string   `json:"image,omitempty"`
	InputParams  []string `json:"inputParams,omitempty"`
	OutputParams []string `json:"outputParams,omitempty"`
	TasksOrSteps []string `json:"tasksOrSteps,omitempty"`
}

type getTemplateOutput struct {
	Name          string                  `json:"name"`
	Namespace     string                  `json:"namespace,omitempty"`
	ClusterScope  bool                    `json:"clusterScope"`
	Description   string                  `json:"description,omitempty"`
	CreatedAt     string                  `json:"createdAt,omitempty"`
	Labels        map[string]string       `json:"labels,omitempty"`
	Annotations   map[string]string       `json:"annotations,omitempty"`
	Entrypoint    string                  `json:"entrypoint,omitempty"`
	Parameters    []templateParameterSpec `json:"parameters,omitempty"`
	Templates     []templateStepSummary   `json:"templates,omitempty"`
	Synchronized  bool                    `json:"hasSynchronization,omitempty"`
	ServiceAcct   string                  `json:"serviceAccountName,omitempty"`
	ActiveTimeout *int64                  `json:"activeDeadlineSeconds,omitempty"`
}

func registerGetTemplate(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "get_template",
		Description: "Get sanitized details, parameter contract (defaults/enums), and template step summary for a WorkflowTemplate or ClusterWorkflowTemplate.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetCore, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in getTemplateInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
			return nil, nil, err
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, in.ClusterScope, meta)
		if err != nil {
			return nil, nil, err
		}
		meta.Resource = strings.TrimSpace(in.Name)

		var (
			objMeta metav1.ObjectMeta
			spec    wfv1.WorkflowSpec
		)
		if in.ClusterScope {
			svc, svcErr := client.ClusterWorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("get_template", svcErr)
			}
			cwft, getErr := svc.GetClusterWorkflowTemplate(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateGetRequest{
				Name: strings.TrimSpace(in.Name),
			})
			if getErr != nil {
				return nil, nil, argo.FormatAgentError("get_template", getErr)
			}
			objMeta = cwft.ObjectMeta
			spec = cwft.Spec
		} else {
			svc, svcErr := client.WorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("get_template", svcErr)
			}
			wft, getErr := svc.GetWorkflowTemplate(ctx, &workflowtemplate.WorkflowTemplateGetRequest{
				Namespace: ns,
				Name:      strings.TrimSpace(in.Name),
			})
			if getErr != nil {
				return nil, nil, argo.FormatAgentError("get_template", getErr)
			}
			objMeta = wft.ObjectMeta
			spec = wft.Spec
		}

		out := &getTemplateOutput{
			Name:          objMeta.Name,
			Namespace:     objMeta.Namespace,
			ClusterScope:  in.ClusterScope,
			Labels:        objMeta.Labels,
			Annotations:   governance.SanitizeAnnotations(objMeta.Annotations),
			Entrypoint:    spec.Entrypoint,
			ServiceAcct:   spec.ServiceAccountName,
			ActiveTimeout: spec.ActiveDeadlineSeconds,
			Synchronized:  spec.Synchronization != nil && (len(spec.Synchronization.Mutexes) > 0 || len(spec.Synchronization.Semaphores) > 0),
		}
		if objMeta.Annotations != nil {
			out.Description = governance.GuardUntrustedSnippet(objMeta.Annotations["workflows.argoproj.io/description"], 300)
		}
		if !objMeta.CreationTimestamp.IsZero() {
			out.CreatedAt = objMeta.CreationTimestamp.Format(time.RFC3339)
		}
		for _, p := range spec.Arguments.Parameters {
			ps := templateParameterSpec{Name: p.Name}
			if p.Value != nil {
				ps.Default = governance.RedactParameterValue(p.Name, string(*p.Value))
			}
			if p.Description != nil {
				ps.Description = string(*p.Description)
			}
			for _, e := range p.Enum {
				ps.Enum = append(ps.Enum, string(e))
			}
			out.Parameters = append(out.Parameters, ps)
		}
		for _, t := range spec.Templates {
			out.Templates = append(out.Templates, summarizeTemplateStep(&t))
		}
		return out, nil, nil
	})
}

func summarizeTemplateStep(t *wfv1.Template) templateStepSummary {
	sum := templateStepSummary{Name: t.Name}
	switch {
	case t.Container != nil:
		sum.Kind = "container"
		sum.Image = t.Container.Image
	case t.Script != nil:
		sum.Kind = "script"
		sum.Image = t.Script.Image
	case t.DAG != nil:
		sum.Kind = "dag"
		for _, task := range t.DAG.Tasks {
			sum.TasksOrSteps = append(sum.TasksOrSteps, task.Name)
		}
	case len(t.Steps) > 0:
		sum.Kind = "steps"
		for _, grp := range t.Steps {
			for _, st := range grp.Steps {
				sum.TasksOrSteps = append(sum.TasksOrSteps, st.Name)
			}
		}
	case t.Suspend != nil:
		sum.Kind = "suspend"
	case t.Resource != nil:
		sum.Kind = "resource"
	case t.HTTP != nil:
		sum.Kind = "http"
	case t.ContainerSet != nil:
		sum.Kind = "containerSet"
	default:
		sum.Kind = "other"
	}
	for _, p := range t.Inputs.Parameters {
		sum.InputParams = append(sum.InputParams, p.Name)
	}
	for _, p := range t.Outputs.Parameters {
		sum.OutputParams = append(sum.OutputParams, p.Name)
	}
	return sum
}

// =============================================================================
// 7. inspect_cron_workflows (core)
// =============================================================================

type inspectCronWorkflowsInput struct {
	TargetInput
	Namespace string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name      string `json:"name,omitempty" jsonschema:"Optional CronWorkflow name; omit to list all CronWorkflows in the namespace"`
	Search    string `json:"search,omitempty" jsonschema:"Optional case-insensitive name filter when listing"`
	Labels    string `json:"labels,omitempty" jsonschema:"Optional Kubernetes label selector when listing"`
}

type cronWorkflowDetail struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	Schedules         []string          `json:"schedules"`
	Timezone          string            `json:"timezone,omitempty"`
	Suspended         bool              `json:"suspended"`
	ConcurrencyPolicy string            `json:"concurrencyPolicy,omitempty"`
	Entrypoint        string            `json:"entrypoint,omitempty"`
	LastScheduledTime string            `json:"lastScheduledTime,omitempty"`
	NextScheduledTime string            `json:"nextScheduledTime,omitempty"`
	ActiveWorkflows   []string          `json:"activeWorkflows,omitempty"`
	SucceededCount    int64             `json:"succeededCount"`
	FailedCount       int64             `json:"failedCount"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	CreatedAt         string            `json:"createdAt,omitempty"`
}

type inspectCronWorkflowsOutput struct {
	Namespace     string               `json:"namespace"`
	Total         int                  `json:"total"`
	CronWorkflows []cronWorkflowDetail `json:"cronWorkflows"`
}

func registerInspectCronWorkflows(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "inspect_cron_workflows",
		Description: "Inspect CronWorkflows (pass name for single detail, or omit name to list all in namespace). Losslessly reads both v3.6 singular spec.schedule and v3.6+/v4 spec.schedules and computes the next scheduled run time.",
		Annotations: readOnlyAnnotations(),
	}
	registerGovernedTool(s, deps, ToolsetCore, tool, "Pass name or narrow with namespace/labels/search.", func(ctx context.Context, _ *mcp.CallToolRequest, in inspectCronWorkflowsInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		cronSvc, err := client.CronWorkflowService()
		if err != nil {
			return nil, nil, argo.FormatAgentError("inspect_cron_workflows", err)
		}

		if name := strings.TrimSpace(in.Name); name != "" {
			if err := argo.ValidatePathSegment(name, "name"); err != nil {
				return nil, nil, err
			}
			cw, getErr := cronSvc.GetCronWorkflow(ctx, &cronworkflow.GetCronWorkflowRequest{
				Namespace: ns,
				Name:      name,
			})
			if getErr != nil {
				return nil, nil, argo.FormatAgentError("inspect_cron_workflows", getErr)
			}
			meta.Resource = cw.Name
			return buildCronDetail(cw), nil, nil
		}

		resp, listErr := cronSvc.ListCronWorkflows(ctx, &cronworkflow.ListCronWorkflowsRequest{
			Namespace:   ns,
			ListOptions: &metav1.ListOptions{LabelSelector: strings.TrimSpace(in.Labels)},
		})
		if listErr != nil {
			return nil, nil, argo.FormatAgentError("inspect_cron_workflows", listErr)
		}

		searchLower := strings.ToLower(strings.TrimSpace(in.Search))
		items := make([]cronWorkflowDetail, 0)
		if resp != nil {
			for i := range resp.Items {
				cw := &resp.Items[i]
				if !deps.ScopeFence.IsNamespaceAllowed(cw.Namespace) {
					continue
				}
				if searchLower != "" && !strings.Contains(strings.ToLower(cw.Name), searchLower) {
					continue
				}
				items = append(items, *buildCronDetail(cw))
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
		meta.Resource = fmt.Sprintf("crons:%d", len(items))
		return &inspectCronWorkflowsOutput{
			Namespace:     ns,
			Total:         len(items),
			CronWorkflows: items,
		}, nil, nil
	})
}

func buildCronDetail(cw *wfv1.CronWorkflow) *cronWorkflowDetail {
	schedules := cw.Spec.GetSchedules()
	d := &cronWorkflowDetail{
		Name:              cw.Name,
		Namespace:         cw.Namespace,
		Schedules:         schedules,
		Timezone:          cw.Spec.Timezone,
		Suspended:         cw.Spec.Suspend,
		ConcurrencyPolicy: string(cw.Spec.ConcurrencyPolicy),
		Entrypoint:        cw.Spec.WorkflowSpec.Entrypoint,
		SucceededCount:    cw.Status.Succeeded,
		FailedCount:       cw.Status.Failed,
		Labels:            cw.Labels,
		Annotations:       governance.SanitizeAnnotations(cw.Annotations),
	}
	if !cw.CreationTimestamp.IsZero() {
		d.CreatedAt = cw.CreationTimestamp.Format(time.RFC3339)
	}
	if cw.Status.LastScheduledTime != nil && !cw.Status.LastScheduledTime.IsZero() {
		d.LastScheduledTime = cw.Status.LastScheduledTime.Format(time.RFC3339)
	}
	for _, ref := range cw.Status.Active {
		d.ActiveWorkflows = append(d.ActiveWorkflows, ref.Name)
	}
	if !cw.Spec.Suspend && len(schedules) > 0 {
		if next := computeNextSchedule(schedules, cw.Spec.Timezone, time.Now()); !next.IsZero() {
			d.NextScheduledTime = next.Format(time.RFC3339)
		}
	}
	return d
}

func computeNextSchedule(schedules []string, tz string, now time.Time) time.Time {
	loc := time.UTC
	if strings.TrimSpace(tz) != "" {
		if loaded, err := time.LoadLocation(strings.TrimSpace(tz)); err == nil {
			loc = loaded
		}
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	ref := now.In(loc)
	var earliest time.Time
	for _, expr := range schedules {
		sched, err := parser.Parse(strings.TrimSpace(expr))
		if err != nil {
			continue
		}
		candidate := sched.Next(ref)
		if earliest.IsZero() || candidate.Before(earliest) {
			earliest = candidate
		}
	}
	return earliest
}

func parsePhaseFilter(phases []string) (map[string]bool, error) {
	if len(phases) == 0 {
		return nil, nil
	}
	valid := map[string]bool{
		"Pending":   true,
		"Running":   true,
		"Succeeded": true,
		"Failed":    true,
		"Error":     true,
	}
	out := make(map[string]bool, len(phases))
	for _, p := range phases {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		if !valid[trimmed] {
			return nil, fmt.Errorf("invalid status %q: must be one of Pending, Running, Succeeded, Failed, Error", trimmed)
		}
		out[trimmed] = true
	}
	return out, nil
}

func parseFlexibleDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.HasSuffix(trimmed, "d") {
		daysStr := strings.TrimSuffix(trimmed, "d")
		var days int
		if _, err := fmt.Sscanf(daysStr, "%d", &days); err == nil && days > 0 {
			return time.Duration(days) * 24 * time.Hour, nil
		}
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("expected positive duration such as 30m, 6h, 24h, or 7d")
	}
	return d, nil
}

func clampLimit(v, def, ceiling int) int {
	if v <= 0 {
		return def
	}
	if v > ceiling {
		return ceiling
	}
	return v
}
