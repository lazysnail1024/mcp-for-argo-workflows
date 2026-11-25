package compat

import (
	"context"
	"fmt"
	"sort"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/workflow/packer"
	wfutil "github.com/argoproj/argo-workflows/v4/workflow/util"
)

// Emissary executor standard container names in Argo Workflows v3.6+.
const (
	ContainerMain = "main"
	ContainerInit = "init"
	ContainerWait = "wait"
)

// DecompressWorkflowNodes transparently inflates status.compressedNodes into status.nodes.
// Direct Kubernetes API fetches do not decompress nodes automatically; calling this ensures
// large v3.6+ workflows expose full node maps identically across both connection modes.
func DecompressWorkflowNodes(ctx context.Context, wf *wfv1.Workflow) error {
	if wf == nil {
		return nil
	}
	if err := packer.DecompressWorkflow(ctx, wf); err != nil {
		return fmt.Errorf("failed to decompress workflow %s/%s nodes: %w", wf.Namespace, wf.Name, err)
	}
	return nil
}

// ResolveNodePodName computes the real Kubernetes Pod name for a workflow node,
// accurately supporting Argo Workflows v3.6+ default PodName v2 (<wf>-<template>-<hash>)
// as well as legacy PodName v1 (<node.ID>).
func ResolveNodePodName(wf *wfv1.Workflow, node *wfv1.NodeStatus) string {
	if wf == nil || node == nil {
		return ""
	}
	version := wfutil.GetWorkflowPodNameVersion(wf)
	tmplName := node.TemplateName
	if tmplName == "" && node.TemplateRef != nil {
		tmplName = node.TemplateRef.Template
	}
	return wfutil.GeneratePodName(wf.Name, node.Name, tmplName, node.ID, version)
}

// FindNode resolves a workflow node by exact ID, exact Name, exact DisplayName,
// resolved PodName v2, or unique case-insensitive display-name suffix match.
func FindNode(wf *wfv1.Workflow, selector string) (*wfv1.NodeStatus, error) {
	if wf == nil || len(wf.Status.Nodes) == 0 {
		return nil, fmt.Errorf("workflow has no recorded nodes")
	}
	needle := strings.TrimSpace(selector)
	if needle == "" {
		return nil, fmt.Errorf("node selector cannot be empty")
	}

	// 1. Exact ID lookup.
	if node, ok := wf.Status.Nodes[needle]; ok {
		n := node
		return &n, nil
	}

	// 2. Exact Name, DisplayName, or computed PodName match.
	keys := make([]string, 0, len(wf.Status.Nodes))
	for id := range wf.Status.Nodes {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	for _, id := range keys {
		node := wf.Status.Nodes[id]
		if node.Name == needle || node.DisplayName == needle {
			n := node
			return &n, nil
		}
		if ResolveNodePodName(wf, &node) == needle {
			n := node
			return &n, nil
		}
	}

	// 3. Case-insensitive DisplayName or TemplateName match (only if unique).
	lower := strings.ToLower(needle)
	var matches []wfv1.NodeStatus
	for _, id := range keys {
		node := wf.Status.Nodes[id]
		if strings.ToLower(node.DisplayName) == lower ||
			strings.ToLower(node.TemplateName) == lower ||
			strings.HasSuffix(strings.ToLower(node.Name), "."+lower) {
			matches = append(matches, node)
		}
	}
	if len(matches) == 1 {
		n := matches[0]
		return &n, nil
	}
	if len(matches) > 1 {
		candidates := make([]string, 0, min(len(matches), 5))
		for i := 0; i < len(matches) && i < 5; i++ {
			candidates = append(candidates, fmt.Sprintf("%s (id=%s)", matches[i].DisplayName, matches[i].ID))
		}
		return nil, fmt.Errorf("node selector %q matched %d nodes (e.g. %s); pass an exact node ID from diagnose_workflow or get_workflow", needle, len(matches), strings.Join(candidates, ", "))
	}

	return nil, fmt.Errorf("node %q not found in workflow %q; call diagnose_workflow or get_workflow to inspect valid node IDs and display names", needle, wf.Name)
}

// CandidateContainersForNode returns the prioritized container order to probe when
// container="auto". Emissary executor runs init (artifact download) -> main (user workload)
// -> wait (artifact upload/output capture).
func CandidateContainersForNode(node *wfv1.NodeStatus) []string {
	if node == nil {
		return []string{ContainerMain, ContainerInit, ContainerWait}
	}
	msg := strings.ToLower(node.Message)
	switch {
	case strings.Contains(msg, "init container") || strings.Contains(msg, "init:") || strings.Contains(msg, "input artifact"):
		return []string{ContainerInit, ContainerMain, ContainerWait}
	case strings.Contains(msg, "wait container") || strings.Contains(msg, "save artifacts") || strings.Contains(msg, "output artifact") || strings.Contains(msg, "output parameter"):
		return []string{ContainerWait, ContainerMain, ContainerInit}
	case node.Outputs != nil && node.Outputs.ExitCode != nil && *node.Outputs.ExitCode == "0" && (node.Phase == wfv1.NodeFailed || node.Phase == wfv1.NodeError):
		// Main exited 0 yet node failed -> typically wait sidecar failed to archive outputs/artifacts.
		return []string{ContainerWait, ContainerMain, ContainerInit}
	default:
		return []string{ContainerMain, ContainerInit, ContainerWait}
	}
}
