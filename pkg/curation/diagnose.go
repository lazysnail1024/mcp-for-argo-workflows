// Package curation implements high-signal SRE workflow diagnosis, noise stripping,
// and compact DAG/Steps topology rendering.
package curation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflow"
	"github.com/argoproj/argo-workflows/v4/pkg/apiclient/workflowarchive"
	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pipekit/mcp-for-argo-workflows/pkg/argo"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/compat"
	"github.com/pipekit/mcp-for-argo-workflows/pkg/governance"
)

const (
	defaultDiagnoseTailLines = int64(40)
	maxDiagnoseTailLines     = int64(200)
	maxRootCauseLogs         = 3
	maxDiagnoseLogPerNode    = 8 * 1024
	diagnoseSubTimeout       = 10 * time.Second
)

// ParameterSummary represents a sanitized workflow or node parameter.
type ParameterSummary struct {
	Name   string `json:"name"`
	Value  string `json:"value,omitempty"`
	Source string `json:"source,omitempty"`
}

// ArtifactSummary represents an input or output artifact reference.
type ArtifactSummary struct {
	Name   string `json:"name"`
	Path   string `json:"path,omitempty"`
	Source string `json:"source,omitempty"`
}

// NodeSummary counts workflow nodes by phase.
type NodeSummary struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Error     int `json:"error"`
	Running   int `json:"running"`
	Pending   int `json:"pending"`
	Skipped   int `json:"skipped"`
	Omitted   int `json:"omitted"`
}

// ContainerStateSummary summarizes an individual Pod container's runtime/termination state.
type ContainerStateSummary struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	Reason     string `json:"reason,omitempty"`
	ExitCode   *int32 `json:"exitCode,omitempty"`
	Message    string `json:"message,omitempty"`
	RestartCnt int32  `json:"restartCount,omitempty"`
}

// ErrorPatternSummary classifies a recognized failure signature with remediation advice.
type ErrorPatternSummary struct {
	Pattern    string `json:"pattern"`
	Suggestion string `json:"suggestion"`
}

// AbnormalNodeDetail captures curated high-signal details for a non-succeeded node.
type AbnormalNodeDetail struct {
	ID                string                  `json:"id"`
	Name              string                  `json:"name"`
	DisplayName       string                  `json:"displayName,omitempty"`
	Type              string                  `json:"type"`
	TemplateName      string                  `json:"templateName,omitempty"`
	PodName           string                  `json:"podName,omitempty"`
	Phase             string                  `json:"phase"`
	Message           string                  `json:"message,omitempty"`
	ExitCode          string                  `json:"exitCode,omitempty"`
	StartedAt         string                  `json:"startedAt,omitempty"`
	FinishedAt        string                  `json:"finishedAt,omitempty"`
	Duration          string                  `json:"duration,omitempty"`
	IsRootCause       bool                    `json:"isRootCause,omitempty"`
	ContainerStatuses []ContainerStateSummary `json:"containerStatuses,omitempty"`
	Inputs            []ParameterSummary      `json:"inputs,omitempty"`
	InputArtifacts    []ArtifactSummary       `json:"inputArtifacts,omitempty"`
	LogContainer      string                  `json:"logContainer,omitempty"`
	Logs              string                  `json:"logs,omitempty"`
	ErrorPattern      *ErrorPatternSummary    `json:"errorPattern,omitempty"`
}

// EventSummary captures a curated Kubernetes event relevant to a workflow or pod.
type EventSummary struct {
	Type           string `json:"type"`
	Reason         string `json:"reason"`
	InvolvedKind   string `json:"involvedKind"`
	InvolvedName   string `json:"involvedName"`
	Message        string `json:"message"`
	Count          int32  `json:"count,omitempty"`
	LastTimestamp  string `json:"lastTimestamp,omitempty"`
	SourceComponet string `json:"sourceComponent,omitempty"`
}

// WorkflowDiagnosis is the structured SRE triage output returned by diagnose_workflow.
type WorkflowDiagnosis struct {
	Name              string               `json:"name"`
	Namespace         string               `json:"namespace"`
	UID               string               `json:"uid,omitempty"`
	Source            string               `json:"source"`
	Phase             string               `json:"phase"`
	Message           string               `json:"message,omitempty"`
	Progress          string               `json:"progress,omitempty"`
	StartedAt         string               `json:"startedAt,omitempty"`
	FinishedAt        string               `json:"finishedAt,omitempty"`
	Duration          string               `json:"duration,omitempty"`
	Parameters        []ParameterSummary   `json:"parameters,omitempty"`
	NodeSummary       *NodeSummary         `json:"nodeSummary,omitempty"`
	RootCauses        []AbnormalNodeDetail `json:"rootCauses,omitempty"`
	CascadingFailures []AbnormalNodeDetail `json:"cascadingFailures,omitempty"`
	PendingNodes      []AbnormalNodeDetail `json:"pendingNodes,omitempty"`
	SuspendHints      []string             `json:"suspendHints,omitempty"`
	WarningEvents     []EventSummary       `json:"warningEvents,omitempty"`
	NextSteps         []string             `json:"nextSteps,omitempty"`
}

// FetchWorkflowTransparent retrieves a workflow by name or UID, automatically decompressing
// status.compressedNodes and falling back to Workflow Archive when connected to Argo Server.
func FetchWorkflowTransparent(
	ctx context.Context,
	client argo.ClientInterface,
	namespace string,
	name string,
	uid string,
) (wf *wfv1.Workflow, source string, err error) {
	wfSvc := client.WorkflowService()

	if strings.TrimSpace(name) != "" {
		liveWf, getErr := wfSvc.GetWorkflow(ctx, &workflow.WorkflowGetRequest{
			Namespace: namespace,
			Name:      name,
		})
		if getErr == nil && liveWf != nil {
			if decErr := compat.DecompressWorkflowNodes(ctx, liveWf); decErr != nil {
				return nil, "", decErr
			}
			return liveWf, "live", nil
		}
		if !client.IsArgoServerMode() || !argo.IsNotFoundError(getErr) {
			return nil, "", getErr
		}

		// Fall back to Workflow Archive by UID if provided, otherwise search archive by name.
		if strings.TrimSpace(uid) == "" {
			archSvc, archErr := client.ArchivedWorkflowService()
			if archErr != nil {
				return nil, "", getErr
			}
			archList, listErr := archSvc.ListArchivedWorkflows(ctx, &workflowarchive.ListArchivedWorkflowsRequest{
				ListOptions: &metav1.ListOptions{
					FieldSelector: fmt.Sprintf("metadata.namespace=%s,metadata.name=%s", namespace, name),
					Limit:         1,
				},
			})
			if listErr != nil || archList == nil || len(archList.Items) == 0 {
				return nil, "", getErr
			}
			uid = string(archList.Items[0].UID)
		}
	}

	if strings.TrimSpace(uid) != "" {
		archSvc, archErr := client.ArchivedWorkflowService()
		if archErr != nil {
			return nil, "", archErr
		}
		archWf, getArchErr := archSvc.GetArchivedWorkflow(ctx, &workflowarchive.GetArchivedWorkflowRequest{
			Uid:       uid,
			Namespace: namespace,
		})
		if getArchErr != nil {
			return nil, "", getArchErr
		}
		if decErr := compat.DecompressWorkflowNodes(ctx, archWf); decErr != nil {
			return nil, "", decErr
		}
		return archWf, "archived", nil
	}

	return nil, "", fmt.Errorf("either name or uid must be provided")
}

// DiagnoseWorkflow runs a bounded concurrent SRE triage across workflow status, DAG root-cause
// topology, Kubernetes Warning events, Pod container statuses (in Direct K8s mode), and
// smart init/main/wait tail logs for root-cause nodes.
func DiagnoseWorkflow(
	ctx context.Context,
	client argo.ClientInterface,
	namespace string,
	name string,
	uid string,
	tailLines int64,
) (*WorkflowDiagnosis, error) {
	if tailLines <= 0 {
		tailLines = defaultDiagnoseTailLines
	}
	if tailLines > maxDiagnoseTailLines {
		tailLines = maxDiagnoseTailLines
	}

	wf, source, err := FetchWorkflowTransparent(ctx, client, namespace, name, uid)
	if err != nil {
		return nil, err
	}

	diag := &WorkflowDiagnosis{
		Name:        wf.Name,
		Namespace:   wf.Namespace,
		UID:         string(wf.UID),
		Source:      source,
		Phase:       NormalizeWorkflowPhase(string(wf.Status.Phase)),
		Message:     governance.GuardUntrustedSnippet(wf.Status.Message, 500),
		Progress:    string(wf.Status.Progress),
		Parameters:  ExtractParameters(wf.Spec.Arguments.Parameters),
		NodeSummary: BuildNodeSummary(wf.Status.Nodes),
	}
	if !wf.Status.StartedAt.Time.IsZero() {
		diag.StartedAt = wf.Status.StartedAt.Format(time.RFC3339)
		endTime := wf.Status.FinishedAt.Time
		if endTime.IsZero() {
			endTime = time.Now()
		}
		diag.Duration = FormatDuration(endTime.Sub(wf.Status.StartedAt.Time))
	}
	if !wf.Status.FinishedAt.Time.IsZero() {
		diag.FinishedAt = wf.Status.FinishedAt.Format(time.RFC3339)
	}

	// Partition non-succeeded nodes into root-cause failures, cascading failures, pending, and suspend hints.
	failedAll := extractNodesByFilter(wf, func(n *wfv1.NodeStatus) bool {
		return n.Phase == wfv1.NodeFailed || n.Phase == wfv1.NodeError
	})
	rootCauses, cascading := splitRootAndCascadingFailures(wf, failedAll)
	pendingNodes := extractNodesByFilter(wf, func(n *wfv1.NodeStatus) bool {
		return n.Phase == wfv1.NodePending
	})
	suspendHints := extractSuspendHints(wf)

	diag.RootCauses = rootCauses
	diag.CascadingFailures = cascading
	diag.PendingNodes = pendingNodes
	diag.SuspendHints = suspendHints

	// Collect Pod names for event & container-status enrichment.
	podNameSet := make(map[string]struct{})
	for i := range diag.RootCauses {
		if diag.RootCauses[i].PodName != "" {
			podNameSet[diag.RootCauses[i].PodName] = struct{}{}
		}
	}
	for i := range diag.PendingNodes {
		if diag.PendingNodes[i].PodName != "" {
			podNameSet[diag.PendingNodes[i].PodName] = struct{}{}
		}
	}
	interestingPods := make([]string, 0, len(podNameSet))
	for p := range podNameSet {
		interestingPods = append(interestingPods, p)
	}
	sort.Strings(interestingPods)

	subCtx, cancel := context.WithTimeout(ctx, diagnoseSubTimeout)
	defer cancel()

	var (
		mu          sync.Mutex
		events      []corev1.Event
		podStatuses map[string]*corev1.Pod
	)

	g, gCtx := errgroup.WithContext(subCtx)
	g.SetLimit(5)

	// 1. Symmetrically fetch Warning events (WatchEvents snapshot in Argo Server mode; K8s List in Direct K8s mode).
	g.Go(func() error {
		evs, evErr := client.ListWorkflowEvents(gCtx, wf.Namespace, wf.Name, interestingPods, true)
		if evErr == nil {
			mu.Lock()
			events = evs
			mu.Unlock()
		}
		return nil
	})

	// 2. Fetch Pod container statuses when in Direct K8s mode (no-op in Argo Server mode).
	if len(interestingPods) > 0 && !client.IsArgoServerMode() {
		g.Go(func() error {
			pods, pErr := client.GetPodStatuses(gCtx, wf.Namespace, interestingPods)
			if pErr == nil && len(pods) > 0 {
				mu.Lock()
				podStatuses = pods
				mu.Unlock()
			}
			return nil
		})
	}

	// 3. Concurrently fetch tail logs for up to top 3 root-cause leaf nodes (smart init/main/wait probing).
	logLimit := min(len(diag.RootCauses), maxRootCauseLogs)
	for i := 0; i < logLimit; i++ {
		idx := i
		nodeDetail := diag.RootCauses[idx]
		wfNode, exists := wf.Status.Nodes[nodeDetail.ID]
		if !exists || (wfNode.Type != wfv1.NodeTypePod && wfNode.Type != wfv1.NodeTypeContainer) {
			continue
		}
		g.Go(func() error {
			containerUsed, wrappedLogs := fetchSmartNodeTailLogs(gCtx, client, wf, &wfNode, tailLines, maxDiagnoseLogPerNode)
			if wrappedLogs != "" {
				mu.Lock()
				diag.RootCauses[idx].LogContainer = containerUsed
				diag.RootCauses[idx].Logs = wrappedLogs
				mu.Unlock()
			}
			return nil
		})
	}

	_ = g.Wait()

	// Enrich Pod container statuses & classify error patterns.
	for i := range diag.RootCauses {
		if pod := podStatuses[diag.RootCauses[i].PodName]; pod != nil {
			diag.RootCauses[i].ContainerStatuses = ExtractPodContainerStatuses(pod)
		}
		diag.RootCauses[i].ErrorPattern = DetectErrorPattern(&diag.RootCauses[i], events)
	}
	for i := range diag.PendingNodes {
		if pod := podStatuses[diag.PendingNodes[i].PodName]; pod != nil {
			diag.PendingNodes[i].ContainerStatuses = ExtractPodContainerStatuses(pod)
		}
		diag.PendingNodes[i].ErrorPattern = DetectErrorPattern(&diag.PendingNodes[i], events)
	}

	diag.WarningEvents = SummarizeEvents(events, 25)
	diag.NextSteps = buildNextStepRecommendations(diag)

	return diag, nil
}

// FetchSmartNodeLogs retrieves tail logs for a node with optional container="auto" fallback
// across init/main/wait containers and wraps them through governance.CleanAndGuardLogs.
func FetchSmartNodeLogs(
	ctx context.Context,
	client argo.ClientInterface,
	wf *wfv1.Workflow,
	node *wfv1.NodeStatus,
	container string,
	tailLines int64,
	grep string,
	maxBytes int,
) (resolvedPod string, resolvedContainer string, wrapped string, truncated bool, err error) {
	resolvedPod = compat.ResolveNodePodName(wf, node)
	if resolvedPod == "" {
		return "", "", "", false, fmt.Errorf("cannot determine pod name for node %q", node.ID)
	}

	candidates := []string{strings.TrimSpace(container)}
	if candidates[0] == "" || strings.EqualFold(candidates[0], "auto") {
		candidates = compat.CandidateContainersForNode(node)
	}

	wfSvc := client.WorkflowService()
	var lastErr error
	for _, c := range candidates {
		raw, recvErr := readPodLogStream(ctx, wfSvc, wf.Namespace, wf.Name, resolvedPod, c, tailLines, grep, maxBytes*2)
		if recvErr != nil {
			lastErr = recvErr
			continue
		}
		if strings.TrimSpace(raw) != "" {
			guarded, trunc := governance.CleanAndGuardLogs(raw, resolvedPod, c, maxBytes)
			return resolvedPod, c, guarded, trunc, nil
		}
	}

	if lastErr != nil && len(candidates) == 1 {
		return resolvedPod, candidates[0], "", false, lastErr
	}
	return resolvedPod, candidates[0], "", false, nil
}

func fetchSmartNodeTailLogs(
	ctx context.Context,
	client argo.ClientInterface,
	wf *wfv1.Workflow,
	node *wfv1.NodeStatus,
	tailLines int64,
	maxBytes int,
) (string, string) {
	_, usedContainer, wrapped, _, err := FetchSmartNodeLogs(ctx, client, wf, node, "auto", tailLines, "", maxBytes)
	if err != nil {
		return "", ""
	}
	return usedContainer, wrapped
}

func readPodLogStream(
	ctx context.Context,
	wfSvc workflow.WorkflowServiceClient,
	namespace string,
	workflowName string,
	podName string,
	container string,
	tailLines int64,
	grep string,
	readBudget int,
) (string, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := &workflow.WorkflowLogRequest{
		Namespace: namespace,
		Name:      workflowName,
		PodName:   podName,
		Grep:      grep,
		LogOptions: &corev1.PodLogOptions{
			Container: container,
			TailLines: &tailLines,
		},
	}

	stream, err := wfSvc.WorkflowLogs(streamCtx, req)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	for {
		entry, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if sb.Len() > 0 {
				break
			}
			return "", recvErr
		}
		if entry == nil {
			continue
		}
		sb.WriteString(entry.Content)
		sb.WriteByte('\n')
		if readBudget > 0 && sb.Len() >= readBudget {
			break
		}
	}
	return sb.String(), nil
}

// BuildNodeSummary computes phase totals across all nodes.
func BuildNodeSummary(nodes wfv1.Nodes) *NodeSummary {
	if len(nodes) == 0 {
		return nil
	}
	summary := &NodeSummary{Total: len(nodes)}
	for _, node := range nodes {
		switch node.Phase {
		case wfv1.NodeSucceeded:
			summary.Succeeded++
		case wfv1.NodeFailed:
			summary.Failed++
		case wfv1.NodeError:
			summary.Error++
		case wfv1.NodeRunning:
			summary.Running++
		case wfv1.NodePending:
			summary.Pending++
		case wfv1.NodeSkipped:
			summary.Skipped++
		case wfv1.NodeOmitted:
			summary.Omitted++
		}
	}
	return summary
}

// ExtractAbnormalNodes builds curated details for every non-succeeded/non-skipped/non-omitted node.
func ExtractAbnormalNodes(wf *wfv1.Workflow) []AbnormalNodeDetail {
	return extractNodesByFilter(wf, func(n *wfv1.NodeStatus) bool {
		switch n.Phase {
		case wfv1.NodeSucceeded, wfv1.NodeSkipped, wfv1.NodeOmitted:
			return false
		default:
			return true
		}
	})
}

func extractNodesByFilter(wf *wfv1.Workflow, keep func(*wfv1.NodeStatus) bool) []AbnormalNodeDetail {
	if wf == nil || len(wf.Status.Nodes) == 0 {
		return nil
	}
	out := make([]AbnormalNodeDetail, 0)
	for _, node := range wf.Status.Nodes {
		n := node
		if !keep(&n) {
			continue
		}
		out = append(out, BuildAbnormalNodeDetail(wf, &n))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt != out[j].StartedAt {
			return out[i].StartedAt < out[j].StartedAt
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// BuildAbnormalNodeDetail converts a workflow NodeStatus into a sanitized, high-signal struct.
func BuildAbnormalNodeDetail(wf *wfv1.Workflow, node *wfv1.NodeStatus) AbnormalNodeDetail {
	detail := AbnormalNodeDetail{
		ID:           node.ID,
		Name:         node.Name,
		DisplayName:  node.DisplayName,
		Type:         string(node.Type),
		TemplateName: node.TemplateName,
		Phase:        NormalizeWorkflowPhase(string(node.Phase)),
		Message:      governance.GuardUntrustedSnippet(node.Message, 500),
	}
	if node.Type == wfv1.NodeTypePod || node.Type == wfv1.NodeTypeContainer {
		detail.PodName = compat.ResolveNodePodName(wf, node)
	}
	if node.Outputs != nil && node.Outputs.ExitCode != nil {
		detail.ExitCode = *node.Outputs.ExitCode
	}
	if !node.StartedAt.Time.IsZero() {
		detail.StartedAt = node.StartedAt.Format(time.RFC3339)
		endTime := node.FinishedAt.Time
		if endTime.IsZero() {
			endTime = time.Now()
		}
		detail.Duration = FormatDuration(endTime.Sub(node.StartedAt.Time))
	}
	if !node.FinishedAt.Time.IsZero() {
		detail.FinishedAt = node.FinishedAt.Format(time.RFC3339)
	}
	if node.Inputs != nil {
		detail.Inputs = ExtractParameters(node.Inputs.Parameters)
		detail.InputArtifacts = ExtractArtifacts(node.Inputs.Artifacts)
	}
	return detail
}

func splitRootAndCascadingFailures(wf *wfv1.Workflow, failed []AbnormalNodeDetail) (roots, cascading []AbnormalNodeDetail) {
	if len(failed) == 0 {
		return nil, nil
	}

	for _, n := range failed {
		wfNode := wf.Status.Nodes[n.ID]
		isRoot := true

		switch wfNode.Type {
		case wfv1.NodeTypePod, wfv1.NodeTypeContainer, wfv1.NodeTypeHTTP, wfv1.NodeTypePlugin:
			// Leaf execution node: root cause unless an upstream sibling failed before it started.
			for _, other := range failed {
				if other.ID == n.ID {
					continue
				}
				otherNode := wf.Status.Nodes[other.ID]
				if !otherNode.FinishedAt.IsZero() && !wfNode.StartedAt.IsZero() &&
					otherNode.FinishedAt.Time.Before(wfNode.StartedAt.Time) &&
					hasDescendant(wf, other.ID, n.ID, map[string]bool{}) {
					isRoot = false
					break
				}
			}
		default:
			// Structural container node (DAG/Steps/StepGroup/Retry): cascading if any
			// transitive descendant or bounded child node failed.
			for _, other := range failed {
				if other.ID == n.ID {
					continue
				}
				otherNode := wf.Status.Nodes[other.ID]
				if otherNode.BoundaryID == n.ID || hasDescendant(wf, n.ID, other.ID, map[string]bool{}) {
					isRoot = false
					break
				}
			}
		}

		if isRoot {
			n.IsRootCause = true
			roots = append(roots, n)
		} else {
			cascading = append(cascading, n)
		}
	}

	if len(roots) == 0 {
		first := failed[0]
		first.IsRootCause = true
		roots = []AbnormalNodeDetail{first}
		if len(failed) > 1 {
			cascading = failed[1:]
		} else {
			cascading = nil
		}
	}
	return roots, cascading
}

func hasDescendant(wf *wfv1.Workflow, nodeID, targetID string, visited map[string]bool) bool {
	if visited[nodeID] {
		return false
	}
	visited[nodeID] = true
	node, ok := wf.Status.Nodes[nodeID]
	if !ok {
		return false
	}
	for _, child := range node.Children {
		if child == targetID || hasDescendant(wf, child, targetID, visited) {
			return true
		}
	}
	return false
}

func extractSuspendHints(wf *wfv1.Workflow) []string {
	var hints []string
	if wf.Spec.Suspend != nil && *wf.Spec.Suspend {
		hints = append(hints, "Workflow spec.suspend=true: execution is intentionally paused; resume via control_workflow(action=\"resume\").")
	}
	for _, node := range wf.Status.Nodes {
		if node.Type == wfv1.NodeTypeSuspend && (node.Phase == wfv1.NodeRunning || node.Phase == wfv1.NodePending) {
			display := node.DisplayName
			if display == "" {
				display = node.Name
			}
			hints = append(hints, fmt.Sprintf(
				"Node %q (id=%s) is paused at a Suspend/approval step (not a crash); resume via control_workflow(action=\"resume\", nodeFieldSelector=\"id=%s\").",
				display, node.ID, node.ID,
			))
		}
	}
	sort.Strings(hints)
	return hints
}

// ExtractPodContainerStatuses converts K8s init/main container statuses into compact summaries.
func ExtractPodContainerStatuses(pod *corev1.Pod) []ContainerStateSummary {
	if pod == nil {
		return nil
	}
	all := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	all = append(all, pod.Status.InitContainerStatuses...)
	all = append(all, pod.Status.ContainerStatuses...)

	out := make([]ContainerStateSummary, 0, len(all))
	for _, cs := range all {
		item := ContainerStateSummary{
			Name:       cs.Name,
			RestartCnt: cs.RestartCount,
		}
		switch {
		case cs.State.Terminated != nil:
			code := cs.State.Terminated.ExitCode
			item.State = "Terminated"
			item.Reason = cs.State.Terminated.Reason
			item.ExitCode = &code
			item.Message = governance.GuardUntrustedSnippet(cs.State.Terminated.Message, 240)
		case cs.State.Waiting != nil:
			item.State = "Waiting"
			item.Reason = cs.State.Waiting.Reason
			item.Message = governance.GuardUntrustedSnippet(cs.State.Waiting.Message, 240)
		case cs.State.Running != nil:
			item.State = "Running"
		default:
			item.State = "Unknown"
		}
		out = append(out, item)
	}
	return out
}

// SummarizeEvents converts raw K8s events into sanitized, bounded EventSummary entries.
func SummarizeEvents(events []corev1.Event, limit int) []EventSummary {
	if len(events) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(events) {
		limit = len(events)
	}
	out := make([]EventSummary, 0, limit)
	for i := 0; i < limit; i++ {
		ev := events[i]
		ts := ""
		switch {
		case !ev.LastTimestamp.IsZero():
			ts = ev.LastTimestamp.Format(time.RFC3339)
		case !ev.EventTime.IsZero():
			ts = ev.EventTime.Format(time.RFC3339)
		case !ev.CreationTimestamp.IsZero():
			ts = ev.CreationTimestamp.Format(time.RFC3339)
		}
		out = append(out, EventSummary{
			Type:           ev.Type,
			Reason:         ev.Reason,
			InvolvedKind:   ev.InvolvedObject.Kind,
			InvolvedName:   ev.InvolvedObject.Name,
			Message:        governance.GuardUntrustedSnippet(ev.Message, 320),
			Count:          ev.Count,
			LastTimestamp:  ts,
			SourceComponet: ev.Source.Component,
		})
	}
	return out
}

// DetectErrorPattern classifies known Argo/Kubernetes failure modes from exit codes,
// container states, node messages, tail logs, and Warning events.
func DetectErrorPattern(node *AbnormalNodeDetail, events []corev1.Event) *ErrorPatternSummary {
	if node == nil {
		return nil
	}
	msg := strings.ToLower(node.Message)
	logs := strings.ToLower(node.Logs)

	// Check pod container status reasons first (most authoritative).
	for _, cs := range node.ContainerStatuses {
		reason := strings.ToLower(cs.Reason)
		switch {
		case reason == "oomkilled" || (cs.ExitCode != nil && *cs.ExitCode == 137):
			return &ErrorPatternSummary{
				Pattern:    fmt.Sprintf("OOMKilled (container=%s, exitCode=137)", cs.Name),
				Suggestion: "Increase resources.limits.memory on the failing template or reduce batch/worker memory footprint.",
			}
		case reason == "imagepullbackoff" || reason == "errimagepull":
			return &ErrorPatternSummary{
				Pattern:    fmt.Sprintf("ImagePullBackOff (container=%s)", cs.Name),
				Suggestion: "Verify container image name/tag, registry reachability, and imagePullSecrets on the Workflow or ServiceAccount.",
			}
		case reason == "createcontainerconfigerror":
			return &ErrorPatternSummary{
				Pattern:    fmt.Sprintf("CreateContainerConfigError (container=%s)", cs.Name),
				Suggestion: "Check referenced ConfigMaps, Secrets, or envFrom keys in the template spec.",
			}
		}
	}

	// Check matching K8s Warning events for scheduling/storage errors.
	for _, ev := range events {
		if node.PodName != "" && ev.InvolvedObject.Name != node.PodName {
			continue
		}
		reason := strings.ToLower(ev.Reason)
		evMsg := strings.ToLower(ev.Message)
		switch {
		case reason == "failedscheduling" || strings.Contains(evMsg, "insufficient"):
			return &ErrorPatternSummary{
				Pattern:    "FailedScheduling (Insufficient cluster resources or affinity mismatch)",
				Suggestion: "Inspect requested CPU/memory/GPU, nodeSelector, and tolerations against available cluster capacity.",
			}
		case reason == "failedmount" || reason == "failedattachvolume":
			return &ErrorPatternSummary{
				Pattern:    "Volume Mount Failure (" + ev.Reason + ")",
				Suggestion: "Verify PVC binding state, storage class availability, and secret/configMap volume references.",
			}
		case reason == "failed" && (strings.Contains(evMsg, "pull") || strings.Contains(evMsg, "manifest unknown")):
			return &ErrorPatternSummary{
				Pattern:    "ImagePullFailure",
				Suggestion: "Confirm the container image exists in the registry and imagePullSecrets are configured.",
			}
		}
	}

	switch node.ExitCode {
	case "137":
		return &ErrorPatternSummary{
			Pattern:    "Exit code 137 (OOMKilled / SIGKILL)",
			Suggestion: "Container was killed (typically OOM). Raise template memory limits or check node eviction events.",
		}
	case "139":
		return &ErrorPatternSummary{
			Pattern:    "Exit code 139 (SIGSEGV)",
			Suggestion: "Native crash / segmentation fault inside workload binary.",
		}
	case "143":
		return &ErrorPatternSummary{
			Pattern:    "Exit code 143 (SIGTERM)",
			Suggestion: "Container received SIGTERM (often activeDeadlineSeconds timeout or workflow stop/termination).",
		}
	}

	switch {
	case strings.Contains(msg, "oomkilled") || strings.Contains(logs, "oomkilled"):
		return &ErrorPatternSummary{
			Pattern:    "OOMKilled",
			Suggestion: "Increase resources.limits.memory for the failing template.",
		}
	case strings.Contains(msg, "imagepullbackoff") || strings.Contains(msg, "errimagepull"):
		return &ErrorPatternSummary{
			Pattern:    "ImagePullBackOff",
			Suggestion: "Verify image tag and imagePullSecrets.",
		}
	case strings.Contains(msg, "deadline exceeded") || strings.Contains(msg, "timeout"):
		return &ErrorPatternSummary{
			Pattern:    "Timeout / activeDeadlineSeconds exceeded",
			Suggestion: "Increase activeDeadlineSeconds or inspect why the step stalled.",
		}
	case node.LogContainer == compat.ContainerInit || strings.Contains(msg, "input artifact"):
		return &ErrorPatternSummary{
			Pattern:    "Init container / input artifact failure",
			Suggestion: "Verify artifact repository credentials, bucket/key path, or upstream step output artifact name.",
		}
	case node.LogContainer == compat.ContainerWait || strings.Contains(msg, "save artifacts") || strings.Contains(msg, "output parameter"):
		return &ErrorPatternSummary{
			Pattern:    "Wait container / output artifact save failure",
			Suggestion: "Check artifact repository write permissions (S3/MinIO/OBS) and verify output file paths exist before main container exits.",
		}
	case strings.Contains(logs, "no space left on device"):
		return &ErrorPatternSummary{
			Pattern:    "Disk space exhausted",
			Suggestion: "Increase ephemeral-storage limits or mount a larger PVC/emptyDir.",
		}
	case strings.Contains(msg, "permission denied") || strings.Contains(logs, "permission denied"):
		return &ErrorPatternSummary{
			Pattern:    "Permission denied",
			Suggestion: "Check ServiceAccount RBAC permissions, container securityContext/runAsUser, and volume permissions.",
		}
	default:
		return nil
	}
}

func buildNextStepRecommendations(diag *WorkflowDiagnosis) []string {
	var steps []string
	if len(diag.SuspendHints) > 0 {
		steps = append(steps, "Workflow contains active Suspend node(s); use control_workflow(action=\"resume\") when ready to proceed.")
	}
	if len(diag.RootCauses) > 0 {
		first := diag.RootCauses[0]
		steps = append(steps, fmt.Sprintf("For deeper logs on root cause %q, call get_workflow_logs(name=%q, node=%q, container=\"auto\").", first.DisplayName, diag.Name, first.ID))
		steps = append(steps, fmt.Sprintf("After resolving the underlying cause, retry from failed step(s) via recover_workflow(name=%q, strategy=\"retry_failed\").", diag.Name))
	} else if len(diag.PendingNodes) > 0 {
		steps = append(steps, fmt.Sprintf("Inspect scheduling or volume warnings with get_workflow_events(name=%q, onlyWarnings=false).", diag.Name))
	}
	return steps
}

// ExtractParameters extracts sanitized parameters (redacting sensitive keys/tokens).
func ExtractParameters(params []wfv1.Parameter) []ParameterSummary {
	if len(params) == 0 {
		return nil
	}
	out := make([]ParameterSummary, 0, len(params))
	for _, p := range params {
		val := ""
		if p.Value != nil {
			val = governance.RedactParameterValue(p.Name, string(*p.Value))
		}
		src := ""
		if p.ValueFrom != nil {
			src = describeValueFrom(p.ValueFrom)
		}
		out = append(out, ParameterSummary{
			Name:   p.Name,
			Value:  governance.GuardUntrustedSnippet(val, 240),
			Source: src,
		})
	}
	return out
}

// ExtractArtifacts extracts concise artifact names, paths, and sanitized locations.
func ExtractArtifacts(arts []wfv1.Artifact) []ArtifactSummary {
	if len(arts) == 0 {
		return nil
	}
	out := make([]ArtifactSummary, 0, len(arts))
	for _, a := range arts {
		src := ""
		switch {
		case a.From != "":
			src = "from:" + a.From
		case a.S3 != nil:
			src = fmt.Sprintf("s3://%s/%s", a.S3.Bucket, a.S3.Key)
		case a.GCS != nil:
			src = fmt.Sprintf("gs://%s/%s", a.GCS.Bucket, a.GCS.Key)
		case a.HTTP != nil:
			src = "http:" + governance.RedactSecretsInText(a.HTTP.URL)
		case a.Git != nil:
			src = "git:" + a.Git.Repo
		}
		out = append(out, ArtifactSummary{
			Name:   a.Name,
			Path:   a.Path,
			Source: src,
		})
	}
	return out
}

func describeValueFrom(vf *wfv1.ValueFrom) string {
	var parts []string
	if vf.Path != "" {
		parts = append(parts, "path:"+vf.Path)
	}
	if vf.Parameter != "" {
		parts = append(parts, "param:"+vf.Parameter)
	}
	if vf.Expression != "" {
		parts = append(parts, "expr:"+vf.Expression)
	}
	if vf.JSONPath != "" {
		parts = append(parts, "jsonpath:"+vf.JSONPath)
	}
	if vf.JQFilter != "" {
		parts = append(parts, "jq:"+vf.JQFilter)
	}
	return strings.Join(parts, ",")
}

// NormalizeWorkflowPhase converts empty phase into "Pending".
func NormalizeWorkflowPhase(phase string) string {
	if strings.TrimSpace(phase) == "" {
		return "Pending"
	}
	return phase
}

// FormatDuration formats a duration concisely for human and LLM readability.
func FormatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm%ds", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}
