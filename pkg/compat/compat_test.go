package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/workflow/packer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNormalizeManifest_V36LegacyCronWorkflow(t *testing.T) {
	raw := `
apiVersion: argoproj.io/v1alpha1
kind: CronWorkflow
metadata:
  name: nightly-etl
  namespace: data
spec:
  schedule: "0 2 * * *"
  timezone: "Asia/Shanghai"
  workflowSpec:
    entrypoint: main
    synchronization:
      mutex:
        name: etl-lock
    templates:
      - name: main
        synchronization:
          semaphore:
            configMapKeyRef:
              name: sem-cm
              key: workflow
        container:
          image: alpine:3.20
          command: [echo, hi]
`
	norm, err := NormalizeManifest(raw)
	require.NoError(t, err)
	assert.Equal(t, KindCronWorkflow, norm.Kind)
	assert.Equal(t, "nightly-etl", norm.Name)
	require.NotNil(t, norm.CronWorkflow)
	assert.Equal(t, []string{"0 2 * * *"}, norm.CronWorkflow.Spec.Schedules)
	require.NotNil(t, norm.CronWorkflow.Spec.WorkflowSpec.Synchronization)
	require.Len(t, norm.CronWorkflow.Spec.WorkflowSpec.Synchronization.Mutexes, 1)
	assert.Equal(t, "etl-lock", norm.CronWorkflow.Spec.WorkflowSpec.Synchronization.Mutexes[0].Name)
	require.Len(t, norm.CronWorkflow.Spec.WorkflowSpec.Templates[0].Synchronization.Semaphores, 1)
	assert.GreaterOrEqual(t, len(norm.Changes), 3)
	assert.NotEmpty(t, norm.Warnings) // missing concurrencyPolicy warning
	assert.Contains(t, norm.CanonicalYAML, "schedules:")
}

func TestNormalizeManifest_RejectsUnknownTypoFields(t *testing.T) {
	raw := `
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: bad-
spec:
  entripoint: main
  templates:
    - name: main
      container:
        image: alpine:3.20
`
	_, err := NormalizeManifest(raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "entripoint")
}

func TestNormalizeManifest_PodPriorityWarning(t *testing.T) {
	raw := `
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: prio-
spec:
  podPriority: 100
  entrypoint: main
  templates:
    - name: main
      container:
        image: alpine:3.20
`
	norm, err := NormalizeManifest(raw)
	require.NoError(t, err)
	require.NotNil(t, norm.Workflow)
	assert.NotEmpty(t, norm.Warnings)
	assert.Contains(t, norm.Warnings[0], "podPriority")
}

func TestNormalizingRoundTripper_UpgradesV36CronJSONAndPropagatesTraceparent(t *testing.T) {
	var seenTraceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenTraceparent = r.Header.Get("traceparent")
		w.Header().Set("Content-Type", "application/json")
		// Argo v3.6 response with singular schedule and mutex (no kind field, matching Argo Server REST).
		_, _ = io.WriteString(w, `{
			"metadata": {"name": "legacy-cron", "namespace": "argo"},
			"spec": {
				"schedule": "*/15 * * * *",
				"workflowSpec": {
					"entrypoint": "main",
					"synchronization": {
						"mutex": {"name": "global-lock"}
					}
				}
			}
		}`)
	}))
	defer srv.Close()

	client := &http.Client{Transport: NewNormalizingRoundTripper(srv.Client().Transport)}
	ctx := WithTraceparent(context.Background(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/cron-workflows/argo/legacy-cron", nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var cw wfv1.CronWorkflow
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&cw))
	assert.Equal(t, []string{"*/15 * * * *"}, cw.Spec.Schedules)
	require.NotNil(t, cw.Spec.WorkflowSpec.Synchronization)
	require.Len(t, cw.Spec.WorkflowSpec.Synchronization.Mutexes, 1)
	assert.Equal(t, "global-lock", cw.Spec.WorkflowSpec.Synchronization.Mutexes[0].Name)
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", seenTraceparent)
}

func TestNormalizingRoundTripper_SkipsStreamingEndpoints(t *testing.T) {
	raw := []byte(`{"result":{"content":"hello schedule mutex"}}`)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/workflows/argo/wf-1/log", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(raw)),
	}
	assert.False(t, shouldNormalizeResponse(req, resp))
}

func TestResolveNodePodName_V2AndV1(t *testing.T) {
	wfV2 := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "my-dag",
			Annotations: map[string]string{"workflows.argoproj.io/pod-name-format": "v2"},
		},
	}
	node := &wfv1.NodeStatus{
		ID:           "my-dag-12345",
		Name:         "my-dag.step-a",
		DisplayName:  "step-a",
		TemplateName: "worker",
		Type:         wfv1.NodeTypePod,
	}
	podV2 := ResolveNodePodName(wfV2, node)
	assert.Contains(t, podV2, "my-dag-worker-")
	assert.NotEqual(t, node.Name, podV2)

	wfV1 := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "my-dag",
			Annotations: map[string]string{"workflows.argoproj.io/pod-name-format": "v1"},
		},
	}
	assert.Equal(t, "my-dag-12345", ResolveNodePodName(wfV1, node))
}

func TestDecompressWorkflowNodes(t *testing.T) {
	ctx := context.Background()
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "big-wf", Namespace: "argo"},
		Status: wfv1.WorkflowStatus{
			Nodes: wfv1.Nodes{
				"big-wf-1": {ID: "big-wf-1", Name: "big-wf.a", DisplayName: "a", Phase: wfv1.NodeFailed},
			},
		},
	}
	restore := packer.SetMaxWorkflowSize(10)
	defer restore()
	// Compress then decompress via our helper.
	require.NoError(t, func() error {
		// Force compression by setting small max size temporarily.
		_ = packer.CompressWorkflowIfNeeded(ctx, wf)
		return nil
	}())
	require.NoError(t, DecompressWorkflowNodes(ctx, wf))
	assert.Contains(t, wf.Status.Nodes, "big-wf-1")
}

func TestCandidateContainersForNode(t *testing.T) {
	exit0 := "0"
	assert.Equal(t, []string{ContainerInit, ContainerMain, ContainerWait}, CandidateContainersForNode(&wfv1.NodeStatus{
		Message: "init container failed to load input artifact",
	}))
	assert.Equal(t, []string{ContainerWait, ContainerMain, ContainerInit}, CandidateContainersForNode(&wfv1.NodeStatus{
		Phase:   wfv1.NodeError,
		Outputs: &wfv1.Outputs{ExitCode: &exit0},
	}))
	assert.Equal(t, []string{ContainerMain, ContainerInit, ContainerWait}, CandidateContainersForNode(&wfv1.NodeStatus{
		Phase:   wfv1.NodeFailed,
		Message: "Error (exit code 1)",
	}))
}
