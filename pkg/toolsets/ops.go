package toolsets

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/cronworkflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowarchive"
	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/utils/ptr"

	"github.com/pipekit/mcp-for-argo-workflows/internal/audit"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/curation"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

// =============================================================================
// 13. trigger_workflow (ops) — strictly template/cron-based, ZERO arbitrary YAML
// =============================================================================

type triggerWorkflowInput struct {
	TargetInput
	Namespace      string            `json:"namespace,omitempty" jsonschema:"Kubernetes namespace to run the workflow in (uses default if omitted)"`
	ResourceKind   string            `json:"resourceKind" jsonschema:"Approved source kind: WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow,required,enum=WorkflowTemplate,enum=ClusterWorkflowTemplate,enum=CronWorkflow"`
	ResourceName   string            `json:"resourceName" jsonschema:"Name of the existing WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow to trigger,required"`
	Parameters     map[string]string `json:"parameters,omitempty" jsonschema:"Parameter overrides as key-value pairs"`
	Labels         map[string]string `json:"labels,omitempty" jsonschema:"Additional Kubernetes labels to attach to the triggered workflow"`
	GenerateName   string            `json:"generateName,omitempty" jsonschema:"Optional metadata.generateName override"`
	Entrypoint     string            `json:"entrypoint,omitempty" jsonschema:"Optional entrypoint template override"`
	ServiceAccount string            `json:"serviceAccount,omitempty" jsonschema:"Optional serviceAccountName override"`
	DryRun         bool              `json:"dryRun,omitempty" jsonschema:"If true, validate submission without creating a live workflow"`
}

type triggerWorkflowOutput struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	UID        string `json:"uid,omitempty"`
	Phase      string `json:"phase"`
	SourceKind string `json:"sourceKind"`
	SourceName string `json:"sourceName"`
	DryRun     bool   `json:"dryRun,omitempty"`
	Message    string `json:"message"`
}

func registerTriggerWorkflow(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "trigger_workflow",
		Description: "Safely trigger a new Argo Workflow from an already-approved WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow with parameter overrides (asynchronous; returns immediately). Does not accept arbitrary raw container YAML—use admin/apply_manifest for raw YAML.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: ptr.To(false),
			IdempotentHint:  false,
			OpenWorldHint:   ptr.To(false),
		},
	}
	registerGovernedTool(s, deps, ToolsetOps, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in triggerWorkflowInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		kind := strings.TrimSpace(in.ResourceKind)
		if kind != "WorkflowTemplate" && kind != "ClusterWorkflowTemplate" && kind != "CronWorkflow" {
			return nil, nil, fmt.Errorf("invalid resourceKind %q: must be WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow", in.ResourceKind)
		}
		if err := argo.ValidatePathSegment(in.ResourceName, "resourceName"); err != nil {
			return nil, nil, err
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}

		submitOpts := &wfv1.SubmitOpts{
			Entrypoint:     strings.TrimSpace(in.Entrypoint),
			GenerateName:   strings.TrimSpace(in.GenerateName),
			ServiceAccount: strings.TrimSpace(in.ServiceAccount),
			DryRun:         in.DryRun,
			Parameters:     mapToKeyValueList(in.Parameters),
			Labels:         mapToCommaLabels(in.Labels),
		}

		created, err := client.WorkflowService().SubmitWorkflow(ctx, &workflow.WorkflowSubmitRequest{
			Namespace:     ns,
			ResourceKind:  kind,
			ResourceName:  strings.TrimSpace(in.ResourceName),
			SubmitOptions: submitOpts,
		})
		if err != nil {
			return nil, nil, argo.FormatAgentError("trigger_workflow", err)
		}
		meta.Resource = created.Name

		msg := fmt.Sprintf("Triggered workflow %s/%s from %s/%s; poll progress via get_workflow or diagnose_workflow.", created.Namespace, created.Name, kind, in.ResourceName)
		if in.DryRun {
			msg = fmt.Sprintf("Dry-run succeeded for %s/%s in namespace %s (no workflow created).", kind, in.ResourceName, ns)
		}

		return &triggerWorkflowOutput{
			Name:       created.Name,
			Namespace:  created.Namespace,
			UID:        string(created.UID),
			Phase:      curation.NormalizeWorkflowPhase(string(created.Status.Phase)),
			SourceKind: kind,
			SourceName: strings.TrimSpace(in.ResourceName),
			DryRun:     in.DryRun,
			Message:    msg,
		}, nil, nil
	})
}

// =============================================================================
// 14. recover_workflow (ops) — unified Live + Archive retry / resubmit
// =============================================================================

type recoverWorkflowInput struct {
	TargetInput
	Namespace         string            `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name              string            `json:"name,omitempty" jsonschema:"Workflow name (provide name or uid)"`
	UID               string            `json:"uid,omitempty" jsonschema:"Workflow UID (supported for archived workflows)"`
	Strategy          string            `json:"strategy,omitempty" jsonschema:"Recovery strategy: retry_failed (default; retries failed/errored nodes in-place) or resubmit (creates a fresh workflow copy),enum=retry_failed,enum=resubmit"`
	NodeFieldSelector string            `json:"nodeFieldSelector,omitempty" jsonschema:"Optional node selector for retry_failed (e.g. displayName=train)"`
	RestartSuccessful bool              `json:"restartSuccessful,omitempty" jsonschema:"For retry_failed: also restart previously succeeded nodes matching nodeFieldSelector (default: false)"`
	Memoized          bool              `json:"memoized,omitempty" jsonschema:"For resubmit: re-use successful memoized steps"`
	Parameters        map[string]string `json:"parameters,omitempty" jsonschema:"Optional parameter overrides for retry or resubmit"`
}

type recoverWorkflowOutput struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid,omitempty"`
	Strategy  string `json:"strategy"`
	Source    string `json:"source"`
	Phase     string `json:"phase"`
	Message   string `json:"message"`
}

func registerRecoverWorkflow(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "recover_workflow",
		Description: "Retry failed steps or resubmit a workflow as a fresh run. Transparently detects whether the workflow is live in Kubernetes or stored in Argo Workflow Archive.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: ptr.To(false),
			IdempotentHint:  false,
			OpenWorldHint:   ptr.To(false),
		},
	}
	registerGovernedTool(s, deps, ToolsetOps, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in recoverWorkflowInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
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

		strategy := strings.ToLower(strings.TrimSpace(in.Strategy))
		if strategy == "" {
			strategy = "retry_failed"
		}
		if strategy != "retry_failed" && strategy != "resubmit" {
			return nil, nil, fmt.Errorf("invalid strategy %q: must be retry_failed or resubmit", in.Strategy)
		}

		wf, source, err := curation.FetchWorkflowTransparent(ctx, client, ns, strings.TrimSpace(in.Name), strings.TrimSpace(in.UID))
		if err != nil {
			return nil, nil, argo.FormatAgentError("recover_workflow", err)
		}
		if err := deps.ScopeFence.CheckNamespace(wf.Namespace); err != nil {
			return nil, nil, err
		}

		params := mapToKeyValueList(in.Parameters)
		var resultWf *wfv1.Workflow

		if source == "archived" {
			archSvc, archErr := client.ArchivedWorkflowService()
			if archErr != nil {
				return nil, nil, argo.FormatAgentError("recover_workflow", archErr)
			}
			if strategy == "resubmit" {
				resultWf, err = archSvc.ResubmitArchivedWorkflow(ctx, &workflowarchive.ResubmitArchivedWorkflowRequest{
					Uid:        string(wf.UID),
					Namespace:  wf.Namespace,
					Memoized:   in.Memoized,
					Parameters: params,
				})
			} else {
				resultWf, err = archSvc.RetryArchivedWorkflow(ctx, &workflowarchive.RetryArchivedWorkflowRequest{
					Uid:               string(wf.UID),
					Namespace:         wf.Namespace,
					RestartSuccessful: in.RestartSuccessful,
					NodeFieldSelector: strings.TrimSpace(in.NodeFieldSelector),
					Parameters:        params,
				})
			}
		} else {
			wfSvc := client.WorkflowService()
			if strategy == "resubmit" {
				resultWf, err = wfSvc.ResubmitWorkflow(ctx, &workflow.WorkflowResubmitRequest{
					Namespace:  wf.Namespace,
					Name:       wf.Name,
					Memoized:   in.Memoized,
					Parameters: params,
				})
			} else {
				resultWf, err = wfSvc.RetryWorkflow(ctx, &workflow.WorkflowRetryRequest{
					Namespace:         wf.Namespace,
					Name:              wf.Name,
					RestartSuccessful: in.RestartSuccessful,
					NodeFieldSelector: strings.TrimSpace(in.NodeFieldSelector),
					Parameters:        params,
				})
			}
		}
		if err != nil {
			return nil, nil, argo.FormatAgentError("recover_workflow", err)
		}

		meta.Namespace = resultWf.Namespace
		meta.Resource = resultWf.Name
		return &recoverWorkflowOutput{
			Name:      resultWf.Name,
			Namespace: resultWf.Namespace,
			UID:       string(resultWf.UID),
			Strategy:  strategy,
			Source:    source,
			Phase:     curation.NormalizeWorkflowPhase(string(resultWf.Status.Phase)),
			Message:   fmt.Sprintf("Executed %s on %s workflow -> active workflow %s/%s.", strategy, source, resultWf.Namespace, resultWf.Name),
		}, nil, nil
	})
}

// =============================================================================
// 15. control_workflow (ops) — suspend / resume (with approval params) / stop / terminate
// =============================================================================

type controlWorkflowInput struct {
	TargetInput
	Namespace         string            `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name              string            `json:"name" jsonschema:"Workflow name,required"`
	Action            string            `json:"action" jsonschema:"Control action: suspend, resume, stop (graceful; runs onExit handlers), or terminate (immediate forced kill; requires confirmation),required,enum=suspend,enum=resume,enum=stop,enum=terminate"`
	NodeFieldSelector string            `json:"nodeFieldSelector,omitempty" jsonschema:"Optional node selector for resume/stop/set (e.g. id=... or displayName=approve)"`
	Message           string            `json:"message,omitempty" jsonschema:"Optional status message recorded on stop/resume"`
	OutputParameters  map[string]string `json:"outputParameters,omitempty" jsonschema:"For resume on a Suspend approval step: output parameter key-values to inject before resuming"`
	ConfirmName       string            `json:"confirmName,omitempty" jsonschema:"Legacy 2025-xx confirmation guard: must match workflow name when action=\"terminate\""`
}

type controlWorkflowOutput struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Action    string `json:"action"`
	Phase     string `json:"phase"`
	Message   string `json:"message"`
}

func registerControlWorkflow(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "control_workflow",
		Description: "Control a running Argo Workflow: suspend, resume (including supplying outputParameters to approve a Suspend step), stop (graceful with onExit handlers), or terminate (forced kill, gated by confirmation).",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: ptr.To(true),
			IdempotentHint:  false,
			OpenWorldHint:   ptr.To(false),
		},
	}
	registerGovernedTool(s, deps, ToolsetOps, tool, "", func(ctx context.Context, req *mcp.CallToolRequest, in controlWorkflowInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
			return nil, nil, err
		}
		action := strings.ToLower(strings.TrimSpace(in.Action))
		if action != "suspend" && action != "resume" && action != "stop" && action != "terminate" {
			return nil, nil, fmt.Errorf("invalid action %q: must be suspend, resume, stop, or terminate", in.Action)
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		wfName := strings.TrimSpace(in.Name)
		meta.Resource = wfName

		// Destructive forced termination skips onExit cleanup handlers -> require confirmation gate.
		if action == "terminate" && deps.MRTRGate != nil {
			challenge, gateErr := deps.MRTRGate.Evaluate(governance.ConfirmationRequest{
				ToolName:    "control_workflow",
				Target:      wfName,
				ConfirmName: in.ConfirmName,
				Summary:     fmt.Sprintf("Immediately TERMINATE workflow %s/%s (skips onExit cleanup handlers; prefer action=\"stop\" when safe)", ns, wfName),
				Digest:      governance.ComputeParamsDigest("terminate", ns, wfName),
				Caller:      audit.ExtractCaller(req),
				Req:         req,
			})
			if gateErr != nil {
				return nil, nil, gateErr
			}
			if challenge != nil {
				return nil, challenge, nil
			}
		}

		wfSvc := client.WorkflowService()
		var updated *wfv1.Workflow

		switch action {
		case "suspend":
			updated, err = wfSvc.SuspendWorkflow(ctx, &workflow.WorkflowSuspendRequest{
				Namespace: ns,
				Name:      wfName,
			})
		case "resume":
			if len(in.OutputParameters) > 0 || strings.TrimSpace(in.Message) != "" {
				var outParams string
				if len(in.OutputParameters) > 0 {
					paramBytes, mErr := json.Marshal(in.OutputParameters)
					if mErr != nil {
						return nil, nil, fmt.Errorf("failed to marshal outputParameters: %w", mErr)
					}
					outParams = string(paramBytes)
				}
				if _, setErr := wfSvc.SetWorkflow(ctx, &workflow.WorkflowSetRequest{
					Namespace:         ns,
					Name:              wfName,
					NodeFieldSelector: strings.TrimSpace(in.NodeFieldSelector),
					Message:           strings.TrimSpace(in.Message),
					OutputParameters:  outParams,
				}); setErr != nil {
					return nil, nil, argo.FormatAgentError("control_workflow(set)", setErr)
				}
			}
			updated, err = wfSvc.ResumeWorkflow(ctx, &workflow.WorkflowResumeRequest{
				Namespace:         ns,
				Name:              wfName,
				NodeFieldSelector: strings.TrimSpace(in.NodeFieldSelector),
			})
		case "stop":
			updated, err = wfSvc.StopWorkflow(ctx, &workflow.WorkflowStopRequest{
				Namespace:         ns,
				Name:              wfName,
				NodeFieldSelector: strings.TrimSpace(in.NodeFieldSelector),
				Message:           strings.TrimSpace(in.Message),
			})
		case "terminate":
			updated, err = wfSvc.TerminateWorkflow(ctx, &workflow.WorkflowTerminateRequest{
				Namespace: ns,
				Name:      wfName,
			})
		}
		if err != nil {
			return nil, nil, argo.FormatAgentError("control_workflow", err)
		}

		return &controlWorkflowOutput{
			Name:      updated.Name,
			Namespace: updated.Namespace,
			Action:    action,
			Phase:     curation.NormalizeWorkflowPhase(string(updated.Status.Phase)),
			Message:   fmt.Sprintf("Workflow %s/%s %s request accepted.", updated.Namespace, updated.Name, action),
		}, nil, nil
	})
}

// =============================================================================
// 16. set_cron_schedule_state (ops)
// =============================================================================

type setCronScheduleStateInput struct {
	TargetInput
	Namespace string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (uses default if omitted)"`
	Name      string `json:"name" jsonschema:"CronWorkflow name,required"`
	Suspended bool   `json:"suspended" jsonschema:"Set true to suspend future scheduled runs, or false to resume scheduling,required"`
}

type setCronScheduleStateOutput struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Schedules []string `json:"schedules"`
	Suspended bool     `json:"suspended"`
	Message   string   `json:"message"`
}

func registerSetCronScheduleState(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "set_cron_schedule_state",
		Description: "Idempotently suspend or resume a CronWorkflow's schedule without affecting already-running workflows.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: ptr.To(false),
			IdempotentHint:  true,
			OpenWorldHint:   ptr.To(false),
		},
	}
	registerGovernedTool(s, deps, ToolsetOps, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in setCronScheduleStateInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
			return nil, nil, err
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, false, meta)
		if err != nil {
			return nil, nil, err
		}
		cronSvc, err := client.CronWorkflowService()
		if err != nil {
			return nil, nil, argo.FormatAgentError("set_cron_schedule_state", err)
		}

		name := strings.TrimSpace(in.Name)
		meta.Resource = name

		var updated *wfv1.CronWorkflow
		if in.Suspended {
			updated, err = cronSvc.SuspendCronWorkflow(ctx, &cronworkflow.CronWorkflowSuspendRequest{
				Namespace: ns,
				Name:      name,
			})
		} else {
			updated, err = cronSvc.ResumeCronWorkflow(ctx, &cronworkflow.CronWorkflowResumeRequest{
				Namespace: ns,
				Name:      name,
			})
		}
		if err != nil {
			return nil, nil, argo.FormatAgentError("set_cron_schedule_state", err)
		}

		stateLabel := "active"
		if updated.Spec.Suspend {
			stateLabel = "suspended"
		}
		return &setCronScheduleStateOutput{
			Name:      updated.Name,
			Namespace: updated.Namespace,
			Schedules: updated.Spec.GetSchedules(),
			Suspended: updated.Spec.Suspend,
			Message:   fmt.Sprintf("CronWorkflow %s/%s is now %s.", updated.Namespace, updated.Name, stateLabel),
		}, nil, nil
	})
}

func mapToKeyValueList(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+m[k])
	}
	return out
}

func mapToCommaLabels(m map[string]string) string {
	return strings.Join(mapToKeyValueList(m), ",")
}
