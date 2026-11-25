package curation

import (
	"testing"
	"time"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSplitRootAndCascadingFailures_AndSuspendHints(t *testing.T) {
	now := time.Now()
	exit137 := "137"
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "ml-train",
			Namespace:   "argo",
			Annotations: map[string]string{"workflows.argoproj.io/pod-name-format": "v2"},
		},
		Status: wfv1.WorkflowStatus{
			Phase: wfv1.WorkflowFailed,
			Nodes: wfv1.Nodes{
				"ml-train": {
					ID:          "ml-train",
					Name:        "ml-train",
					DisplayName: "ml-train",
					Type:        wfv1.NodeTypeDAG,
					Phase:       wfv1.NodeFailed,
					Children:    []string{"ml-train-prep", "ml-train-train", "ml-train-approve"},
					StartedAt:   metav1.Time{Time: now.Add(-5 * time.Minute)},
				},
				"ml-train-prep": {
					ID:           "ml-train-prep",
					Name:         "ml-train.prep",
					DisplayName:  "prep",
					TemplateName: "prep",
					Type:         wfv1.NodeTypePod,
					Phase:        wfv1.NodeSucceeded,
					StartedAt:    metav1.Time{Time: now.Add(-5 * time.Minute)},
					FinishedAt:   metav1.Time{Time: now.Add(-4 * time.Minute)},
				},
				"ml-train-train": {
					ID:           "ml-train-train",
					Name:         "ml-train.train",
					DisplayName:  "train",
					TemplateName: "gpu-train",
					Type:         wfv1.NodeTypePod,
					Phase:        wfv1.NodeFailed,
					Message:      "OOMKilled",
					Outputs:      &wfv1.Outputs{ExitCode: &exit137},
					StartedAt:    metav1.Time{Time: now.Add(-4 * time.Minute)},
					FinishedAt:   metav1.Time{Time: now.Add(-2 * time.Minute)},
				},
				"ml-train-approve": {
					ID:          "ml-train-approve",
					Name:        "ml-train.approve",
					DisplayName: "approve-release",
					Type:        wfv1.NodeTypeSuspend,
					Phase:       wfv1.NodeRunning,
					StartedAt:   metav1.Time{Time: now.Add(-1 * time.Minute)},
				},
			},
		},
	}

	failed := extractNodesByFilter(wf, func(n *wfv1.NodeStatus) bool {
		return n.Phase == wfv1.NodeFailed || n.Phase == wfv1.NodeError
	})
	roots, cascading := splitRootAndCascadingFailures(wf, failed)
	require.Len(t, roots, 1)
	assert.Equal(t, "ml-train-train", roots[0].ID)
	assert.Contains(t, roots[0].PodName, "ml-train-gpu-train-")
	assert.True(t, roots[0].IsRootCause)

	require.Len(t, cascading, 1)
	assert.Equal(t, "ml-train", cascading[0].ID)

	pat := DetectErrorPattern(&roots[0], []corev1.Event{
		{
			Type:           corev1.EventTypeWarning,
			Reason:         "OOMKilling",
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: roots[0].PodName},
			Message:        "Memory cgroup out of memory",
		},
	})
	require.NotNil(t, pat)
	assert.Contains(t, pat.Pattern, "137")

	hints := extractSuspendHints(wf)
	require.Len(t, hints, 1)
	assert.Contains(t, hints[0], "approve-release")
	assert.Contains(t, hints[0], "control_workflow")

	// Verify abnormal extraction strips succeeded prep step.
	abn := ExtractAbnormalNodes(wf)
	for _, n := range abn {
		assert.NotEqual(t, "ml-train-prep", n.ID)
	}
}

func TestSplitRootAndCascadingFailures_ChainedDAGDescendant(t *testing.T) {
	// Models real Argo Workflows v3.6+ DAG structure where root DAG's direct Children
	// only point to the entry task (prep-ok, Succeeded), and prep-ok's Children point to boom-fail.
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "chained-dag"},
		Status: wfv1.WorkflowStatus{
			Phase: wfv1.WorkflowFailed,
			Nodes: wfv1.Nodes{
				"chained-dag": {
					ID:       "chained-dag",
					Type:     wfv1.NodeTypeDAG,
					Phase:    wfv1.NodeFailed,
					Children: []string{"prep-ok"},
				},
				"prep-ok": {
					ID:         "prep-ok",
					BoundaryID: "chained-dag",
					Type:       wfv1.NodeTypePod,
					Phase:      wfv1.NodeSucceeded,
					Children:   []string{"boom-fail"},
				},
				"boom-fail": {
					ID:          "boom-fail",
					DisplayName: "boom-fail",
					BoundaryID:  "chained-dag",
					Type:        wfv1.NodeTypePod,
					Phase:       wfv1.NodeFailed,
				},
			},
		},
	}

	failed := extractNodesByFilter(wf, func(n *wfv1.NodeStatus) bool {
		return n.Phase == wfv1.NodeFailed || n.Phase == wfv1.NodeError
	})
	roots, cascading := splitRootAndCascadingFailures(wf, failed)
	require.Len(t, roots, 1)
	assert.Equal(t, "boom-fail", roots[0].ID)
	require.Len(t, cascading, 1)
	assert.Equal(t, "chained-dag", cascading[0].ID)
}

func TestRenderWorkflowGraph_MermaidAndCollapseSucceeded(t *testing.T) {
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "dag-1"},
		Status: wfv1.WorkflowStatus{
			Nodes: wfv1.Nodes{
				"root":   {ID: "root", DisplayName: "pipeline(main)", Type: wfv1.NodeTypeDAG, Phase: wfv1.NodeFailed, Children: []string{"ok-1", "ok-2", "fail-1"}},
				"ok-1":   {ID: "ok-1", DisplayName: "step-ok-1", Type: wfv1.NodeTypePod, Phase: wfv1.NodeSucceeded},
				"ok-2":   {ID: "ok-2", DisplayName: "step-ok-2", Type: wfv1.NodeTypePod, Phase: wfv1.NodeSucceeded},
				"fail-1": {ID: "fail-1", DisplayName: "step-fail", Type: wfv1.NodeTypePod, Phase: wfv1.NodeFailed},
			},
		},
	}

	res, err := RenderWorkflowGraph(wf, GraphFormatMermaid, true, true)
	require.NoError(t, err)
	assert.Equal(t, 2, res.CollapsedCount)
	assert.Contains(t, res.Graph, "flowchart TD")
	assert.Contains(t, res.Graph, `CollapsedSucceeded["Succeeded leaf steps collapsed: 2"]`)
	assert.Contains(t, res.Graph, `n_fail_1["step-fail"]:::failed`)
	assert.NotContains(t, res.Graph, "step-ok-1")

	asciiRes, err := RenderWorkflowGraph(wf, GraphFormatASCII, true, false)
	require.NoError(t, err)
	assert.Contains(t, asciiRes.Graph, "step-fail [Failed]")
	assert.Contains(t, asciiRes.Graph, "step-ok-1 [Succeeded]")
}
