package toolsets

import (
	"context"
	"fmt"
	"strings"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/clusterworkflowtemplate"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/cronworkflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowarchive"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowtemplate"
	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"

	"github.com/pipekit/mcp-for-argo-workflows/internal/audit"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/compat"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/curation"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

// =============================================================================
// 17. apply_manifest (admin) — raw Workflow submission & declarative Upsert
// =============================================================================

type applyManifestInput struct {
	TargetInput
	Namespace string            `json:"namespace,omitempty" jsonschema:"Target namespace override (ignored for ClusterWorkflowTemplate)"`
	Manifest  string            `json:"manifest" jsonschema:"YAML or JSON manifest (Workflow, WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow),required"`
	Labels    map[string]string `json:"labels,omitempty" jsonschema:"Optional additional labels to merge into metadata.labels"`
	SkipLint  bool              `json:"skipLint,omitempty" jsonschema:"Skip server-side lint preflight before applying (default: false)"`
}

type applyManifestOutput struct {
	Kind               string   `json:"kind"`
	Name               string   `json:"name"`
	Namespace          string   `json:"namespace,omitempty"`
	UID                string   `json:"uid,omitempty"`
	Operation          string   `json:"operation"`
	Phase              string   `json:"phase,omitempty"`
	NormalizationNotes []string `json:"normalizationNotes,omitempty"`
	Warnings           []string `json:"warnings,omitempty"`
	Message            string   `json:"message"`
}

func registerApplyManifest(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "apply_manifest",
		Description: "High-privilege admin tool: submit a raw Workflow YAML or declaratively create/update a WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow. Automatically normalizes v3.6+ legacy fields and runs server-side lint preflight.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: ptr.To(true),
			IdempotentHint:  false,
			OpenWorldHint:   ptr.To(false),
		},
	}
	registerGovernedTool(s, deps, ToolsetAdmin, tool, "", func(ctx context.Context, _ *mcp.CallToolRequest, in applyManifestInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		norm, err := compat.NormalizeManifest(in.Manifest)
		if err != nil {
			return nil, nil, err
		}

		isCluster := norm.Kind == compat.KindClusterWorkflowTemplate
		if isCluster {
			if err := deps.ScopeFence.CheckClusterTemplateMutation(); err != nil {
				return nil, nil, err
			}
		}

		reqNS := strings.TrimSpace(in.Namespace)
		if reqNS == "" {
			reqNS = norm.Namespace
		}
		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, reqNS, isCluster, meta)
		if err != nil {
			return nil, nil, err
		}

		if !in.SkipLint {
			if lintErr := runServerSideLint(ctx, client, norm, ns); lintErr != nil {
				return nil, nil, fmt.Errorf("preflight lint failed before apply: %w", lintErr)
			}
		}

		switch norm.Kind {
		case compat.KindWorkflow:
			wf := norm.Workflow.DeepCopy()
			wf.Namespace = ns
			mergeLabels(&wf.Labels, in.Labels)
			created, cErr := client.WorkflowService().CreateWorkflow(ctx, &workflow.WorkflowCreateRequest{
				Namespace: ns,
				Workflow:  wf,
			})
			if cErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(Workflow)", cErr)
			}
			meta.Resource = created.Name
			return &applyManifestOutput{
				Kind:               compat.KindWorkflow,
				Name:               created.Name,
				Namespace:          created.Namespace,
				UID:                string(created.UID),
				Operation:          "created",
				Phase:              curation.NormalizeWorkflowPhase(string(created.Status.Phase)),
				NormalizationNotes: norm.Changes,
				Warnings:           norm.Warnings,
				Message:            fmt.Sprintf("Submitted Workflow %s/%s.", created.Namespace, created.Name),
			}, nil, nil

		case compat.KindWorkflowTemplate:
			wft := norm.WorkflowTemplate.DeepCopy()
			if err := argo.ValidatePathSegment(wft.Name, "metadata.name"); err != nil {
				return nil, nil, err
			}
			wft.Namespace = ns
			mergeLabels(&wft.Labels, in.Labels)
			svc, svcErr := client.WorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(WorkflowTemplate)", svcErr)
			}
			res, op, uErr := upsertWorkflowTemplate(ctx, svc, ns, wft)
			if uErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(WorkflowTemplate)", uErr)
			}
			meta.Resource = res.Name
			return &applyManifestOutput{
				Kind:               compat.KindWorkflowTemplate,
				Name:               res.Name,
				Namespace:          res.Namespace,
				UID:                string(res.UID),
				Operation:          op,
				NormalizationNotes: norm.Changes,
				Warnings:           norm.Warnings,
				Message:            fmt.Sprintf("WorkflowTemplate %s/%s %s.", res.Namespace, res.Name, op),
			}, nil, nil

		case compat.KindClusterWorkflowTemplate:
			cwft := norm.ClusterWorkflowTemplate.DeepCopy()
			if err := argo.ValidatePathSegment(cwft.Name, "metadata.name"); err != nil {
				return nil, nil, err
			}
			mergeLabels(&cwft.Labels, in.Labels)
			svc, svcErr := client.ClusterWorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(ClusterWorkflowTemplate)", svcErr)
			}
			res, op, uErr := upsertClusterWorkflowTemplate(ctx, svc, cwft)
			if uErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(ClusterWorkflowTemplate)", uErr)
			}
			meta.Resource = res.Name
			return &applyManifestOutput{
				Kind:               compat.KindClusterWorkflowTemplate,
				Name:               res.Name,
				UID:                string(res.UID),
				Operation:          op,
				NormalizationNotes: norm.Changes,
				Warnings:           norm.Warnings,
				Message:            fmt.Sprintf("ClusterWorkflowTemplate %s %s.", res.Name, op),
			}, nil, nil

		case compat.KindCronWorkflow:
			cw := norm.CronWorkflow.DeepCopy()
			if err := argo.ValidatePathSegment(cw.Name, "metadata.name"); err != nil {
				return nil, nil, err
			}
			if len(cw.Spec.GetSchedules()) == 0 {
				return nil, nil, fmt.Errorf("CronWorkflow requires at least one schedule in spec.schedules")
			}
			cw.Namespace = ns
			mergeLabels(&cw.Labels, in.Labels)
			svc, svcErr := client.CronWorkflowService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(CronWorkflow)", svcErr)
			}
			res, op, uErr := upsertCronWorkflow(ctx, svc, ns, cw)
			if uErr != nil {
				return nil, nil, argo.FormatAgentError("apply_manifest(CronWorkflow)", uErr)
			}
			meta.Resource = res.Name
			return &applyManifestOutput{
				Kind:               compat.KindCronWorkflow,
				Name:               res.Name,
				Namespace:          res.Namespace,
				UID:                string(res.UID),
				Operation:          op,
				NormalizationNotes: norm.Changes,
				Warnings:           norm.Warnings,
				Message:            fmt.Sprintf("CronWorkflow %s/%s %s (schedules: %s).", res.Namespace, res.Name, op, strings.Join(res.Spec.GetSchedules(), ", ")),
			}, nil, nil

		default:
			return nil, nil, fmt.Errorf("unsupported kind %q", norm.Kind)
		}
	})
}

func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsAlreadyExists(err) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}

func upsertWorkflowTemplate(ctx context.Context, svc workflowtemplate.WorkflowTemplateServiceClient, ns string, wft *wfv1.WorkflowTemplate) (*wfv1.WorkflowTemplate, string, error) {
	created, err := svc.CreateWorkflowTemplate(ctx, &workflowtemplate.WorkflowTemplateCreateRequest{
		Namespace: ns,
		Template:  wft,
	})
	if err == nil {
		return created, "created", nil
	}
	if !isAlreadyExistsErr(err) {
		return nil, "", err
	}
	existing, getErr := svc.GetWorkflowTemplate(ctx, &workflowtemplate.WorkflowTemplateGetRequest{
		Namespace: ns,
		Name:      wft.Name,
	})
	if getErr != nil {
		return nil, "", getErr
	}
	wft.ResourceVersion = existing.ResourceVersion
	updated, updErr := svc.UpdateWorkflowTemplate(ctx, &workflowtemplate.WorkflowTemplateUpdateRequest{
		Namespace: ns,
		Name:      wft.Name,
		Template:  wft,
	})
	return updated, "updated", updErr
}

func upsertClusterWorkflowTemplate(ctx context.Context, svc clusterworkflowtemplate.ClusterWorkflowTemplateServiceClient, cwft *wfv1.ClusterWorkflowTemplate) (*wfv1.ClusterWorkflowTemplate, string, error) {
	created, err := svc.CreateClusterWorkflowTemplate(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateCreateRequest{
		Template: cwft,
	})
	if err == nil {
		return created, "created", nil
	}
	if !isAlreadyExistsErr(err) {
		return nil, "", err
	}
	existing, getErr := svc.GetClusterWorkflowTemplate(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateGetRequest{
		Name: cwft.Name,
	})
	if getErr != nil {
		return nil, "", getErr
	}
	cwft.ResourceVersion = existing.ResourceVersion
	updated, updErr := svc.UpdateClusterWorkflowTemplate(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateUpdateRequest{
		Name:     cwft.Name,
		Template: cwft,
	})
	return updated, "updated", updErr
}

func upsertCronWorkflow(ctx context.Context, svc cronworkflow.CronWorkflowServiceClient, ns string, cw *wfv1.CronWorkflow) (*wfv1.CronWorkflow, string, error) {
	created, err := svc.CreateCronWorkflow(ctx, &cronworkflow.CreateCronWorkflowRequest{
		Namespace:    ns,
		CronWorkflow: cw,
	})
	if err == nil {
		return created, "created", nil
	}
	if !isAlreadyExistsErr(err) {
		return nil, "", err
	}
	existing, getErr := svc.GetCronWorkflow(ctx, &cronworkflow.GetCronWorkflowRequest{
		Namespace: ns,
		Name:      cw.Name,
	})
	if getErr != nil {
		return nil, "", getErr
	}
	cw.ResourceVersion = existing.ResourceVersion
	updated, updErr := svc.UpdateCronWorkflow(ctx, &cronworkflow.UpdateCronWorkflowRequest{
		Namespace:    ns,
		Name:         cw.Name,
		CronWorkflow: cw,
	})
	return updated, "updated", updErr
}

func mergeLabels(dst *map[string]string, extra map[string]string) {
	if len(extra) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]string, len(extra))
	}
	for k, v := range extra {
		(*dst)[k] = v
	}
}

// =============================================================================
// 18. delete_resource (admin) — MRTR / confirmName gated destruction
// =============================================================================

type deleteResourceInput struct {
	TargetInput
	Kind        string `json:"kind" jsonschema:"Resource kind to delete: Workflow, ArchivedWorkflow, WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow,required,enum=Workflow,enum=ArchivedWorkflow,enum=WorkflowTemplate,enum=ClusterWorkflowTemplate,enum=CronWorkflow"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"Kubernetes namespace (ignored for ClusterWorkflowTemplate and ArchivedWorkflow)"`
	Name        string `json:"name,omitempty" jsonschema:"Resource name (required for Workflow, WorkflowTemplate, ClusterWorkflowTemplate, CronWorkflow)"`
	UID         string `json:"uid,omitempty" jsonschema:"Archived workflow UID (required when kind=\"ArchivedWorkflow\")"`
	ConfirmName string `json:"confirmName,omitempty" jsonschema:"Legacy 2025-xx confirmation guard: must match target name (or uid for ArchivedWorkflow)"`
}

type deleteResourceOutput struct {
	Kind      string `json:"kind"`
	Name      string `json:"name,omitempty"`
	UID       string `json:"uid,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Deleted   bool   `json:"deleted"`
	Message   string `json:"message"`
}

func registerDeleteResource(s *mcp.Server, deps Dependencies) {
	tool := &mcp.Tool{
		Name:        "delete_resource",
		Description: "High-privilege admin tool: delete an Argo Workflow, ArchivedWorkflow, WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow. Always gated by dual-era confirmation (2026-07-28 MRTR approval or 2025-xx confirmName). Archived workflow deletion is disabled unless MCP_ALLOW_ARCHIVE_DELETE=true.",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: ptr.To(true),
			IdempotentHint:  true,
			OpenWorldHint:   ptr.To(false),
		},
	}
	registerGovernedTool(s, deps, ToolsetAdmin, tool, "", func(ctx context.Context, req *mcp.CallToolRequest, in deleteResourceInput, meta *ExecutionMeta) (any, *mcp.CallToolResult, error) {
		kind := strings.TrimSpace(in.Kind)
		var targetID string
		var isCluster bool

		switch kind {
		case compat.KindWorkflow, compat.KindWorkflowTemplate, compat.KindCronWorkflow:
			if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
				return nil, nil, err
			}
			targetID = strings.TrimSpace(in.Name)
		case compat.KindClusterWorkflowTemplate:
			if err := deps.ScopeFence.CheckClusterTemplateMutation(); err != nil {
				return nil, nil, err
			}
			if err := argo.ValidatePathSegment(in.Name, "name"); err != nil {
				return nil, nil, err
			}
			targetID = strings.TrimSpace(in.Name)
			isCluster = true
		case "ArchivedWorkflow":
			if err := deps.ScopeFence.CheckArchiveDelete(); err != nil {
				return nil, nil, err
			}
			if err := argo.ValidatePathSegment(in.UID, "uid"); err != nil {
				return nil, nil, err
			}
			targetID = strings.TrimSpace(in.UID)
			isCluster = true
		default:
			return nil, nil, fmt.Errorf("invalid kind %q: must be Workflow, ArchivedWorkflow, WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow", in.Kind)
		}

		ctx, client, ns, err := resolveClientAndNamespace(ctx, deps, in.TargetInput, in.Namespace, isCluster, meta)
		if err != nil {
			return nil, nil, err
		}
		meta.Resource = kind + "/" + targetID

		scopeLabel := targetID
		if !isCluster {
			scopeLabel = ns + "/" + targetID
		}
		if deps.MRTRGate != nil {
			challenge, gateErr := deps.MRTRGate.Evaluate(governance.ConfirmationRequest{
				ToolName:    "delete_resource",
				Target:      targetID,
				ConfirmName: in.ConfirmName,
				Summary:     fmt.Sprintf("Permanently DELETE %s %s", kind, scopeLabel),
				Digest:      governance.ComputeParamsDigest("delete", kind, ns, targetID),
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

		switch kind {
		case compat.KindWorkflow:
			_, err = client.WorkflowService().DeleteWorkflow(ctx, &workflow.WorkflowDeleteRequest{
				Namespace: ns,
				Name:      targetID,
			})
		case "ArchivedWorkflow":
			archSvc, svcErr := client.ArchivedWorkflowService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("delete_resource(ArchivedWorkflow)", svcErr)
			}
			_, err = archSvc.DeleteArchivedWorkflow(ctx, &workflowarchive.DeleteArchivedWorkflowRequest{
				Uid: targetID,
			})
		case compat.KindWorkflowTemplate:
			svc, svcErr := client.WorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("delete_resource(WorkflowTemplate)", svcErr)
			}
			_, err = svc.DeleteWorkflowTemplate(ctx, &workflowtemplate.WorkflowTemplateDeleteRequest{
				Namespace: ns,
				Name:      targetID,
			})
		case compat.KindClusterWorkflowTemplate:
			svc, svcErr := client.ClusterWorkflowTemplateService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("delete_resource(ClusterWorkflowTemplate)", svcErr)
			}
			_, err = svc.DeleteClusterWorkflowTemplate(ctx, &clusterworkflowtemplate.ClusterWorkflowTemplateDeleteRequest{
				Name: targetID,
			})
		case compat.KindCronWorkflow:
			svc, svcErr := client.CronWorkflowService()
			if svcErr != nil {
				return nil, nil, argo.FormatAgentError("delete_resource(CronWorkflow)", svcErr)
			}
			_, err = svc.DeleteCronWorkflow(ctx, &cronworkflow.DeleteCronWorkflowRequest{
				Namespace: ns,
				Name:      targetID,
			})
		}
		if err != nil {
			return nil, nil, argo.FormatAgentError("delete_resource", err)
		}

		out := &deleteResourceOutput{
			Kind:    kind,
			Deleted: true,
			Message: fmt.Sprintf("Deleted %s %s.", kind, scopeLabel),
		}
		if kind == "ArchivedWorkflow" {
			out.UID = targetID
		} else {
			out.Name = targetID
			if !isCluster {
				out.Namespace = ns
			}
		}
		return out, nil, nil
	})
}
