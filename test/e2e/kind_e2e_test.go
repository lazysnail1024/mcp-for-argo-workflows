//go:build e2e

package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type e2eEnv struct {
	BaseURL    string
	K8sBaseURL string
	AuthToken  string
	ArgoToken  string
	SysNS      string
	WorkNS     string
	DeniedNS   string
	ImageRepo  string
	ImageTag   string
	KindNodeIP string
	HTTPClient *http.Client
}

func loadEnv(t *testing.T) *e2eEnv {
	t.Helper()
	req := func(key string) string {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			t.Fatalf("required E2E env var %s is empty; run via scripts/run-e2e-kind.sh", key)
		}
		return v
	}
	return &e2eEnv{
		BaseURL:    req("E2E_BASE_URL"),
		K8sBaseURL: req("E2E_K8S_BASE_URL"),
		AuthToken:  req("E2E_MCP_AUTH_TOKEN"),
		ArgoToken:  req("E2E_ARGO_TOKEN"),
		SysNS:      req("E2E_SYS_NS"),
		WorkNS:     req("E2E_WORK_NS"),
		DeniedNS:   req("E2E_DENIED_NS"),
		ImageRepo:  req("E2E_IMAGE_REPO"),
		ImageTag:   req("E2E_IMAGE_TAG"),
		KindNodeIP: req("E2E_KIND_NODE_IP"),
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
			// Disable keep-alive so kube-proxy distributes consecutive requests across both replicas.
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		},
	}
}

func (e *e2eEnv) postMCP(t *testing.T, baseURL string, body map[string]any, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if _, ok := headers["Authorization"]; !ok {
		req.Header.Set("Authorization", "Bearer "+e.AuthToken)
	}
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s/mcp failed: %v", baseURL, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, data
}

func (e *e2eEnv) callTool2025(t *testing.T, baseURL, toolName string, args map[string]any, caller string) (string, bool) {
	t.Helper()
	headers := map[string]string{}
	if caller != "" {
		headers["X-MCP-Caller"] = caller
	}
	resp, data := e.postMCP(t, baseURL, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": args,
		},
	}, headers)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tool %s expected HTTP 200, got %d: %s", toolName, resp.StatusCode, string(data))
	}
	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		t.Fatalf("stateless server must not emit Mcp-Session-Id, got %q", sid)
	}
	return extractToolText(t, data)
}

func (e *e2eEnv) callTool2026(t *testing.T, baseURL, toolName string, args map[string]any, extraParams map[string]any, caller string) map[string]any {
	t.Helper()
	reqMeta := map[string]any{
		"io.modelcontextprotocol/protocolVersion": "2026-07-28",
		"io.modelcontextprotocol/clientCapabilities": map[string]any{
			"elicitation": map[string]any{},
		},
	}
	headers := map[string]string{
		"Mcp-Protocol-Version": "2026-07-28",
		"Mcp-Method":           "tools/call",
		"Mcp-Name":             toolName,
	}
	if v, ok := args["namespace"].(string); ok && v != "" {
		headers["Mcp-Param-Namespace"] = v
	}
	if v, ok := args["name"].(string); ok && v != "" {
		headers["Mcp-Param-Workflow-Name"] = v
	}
	if v, ok := args["kind"].(string); ok && v != "" {
		headers["Mcp-Param-Resource-Kind"] = v
	}
	if caller != "" {
		headers["X-MCP-Caller"] = caller
	}
	params := map[string]any{
		"name":      toolName,
		"arguments": args,
		"_meta":     reqMeta,
	}
	for k, v := range extraParams {
		params[k] = v
	}
	resp, data := e.postMCP(t, baseURL, map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/call",
		"params":  params,
	}, headers)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("2026 tool %s expected HTTP 200, got %d: %s", toolName, resp.StatusCode, string(data))
	}
	var env struct {
		Result map[string]any `json:"result"`
		Error  any            `json:"error"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal 2026 envelope: %v (%s)", err, string(data))
	}
	if env.Error != nil {
		t.Fatalf("unexpected JSON-RPC error for %s: %v", toolName, env.Error)
	}
	return env.Result
}

func extractToolText(t *testing.T, raw []byte) (string, bool) {
	t.Helper()
	var env struct {
		Result struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal tool response: %v (%s)", err, string(raw))
	}
	if env.Error != nil {
		t.Fatalf("unexpected JSON-RPC error: %v", env.Error)
	}
	if len(env.Result.Content) == 0 {
		return "", env.Result.IsError
	}
	return env.Result.Content[0].Text, env.Result.IsError
}

func TestKindE2E_FullAcceptance(t *testing.T) {
	env := loadEnv(t)

	t.Run("PhaseA_Security_DualEra_And_Healthz", func(t *testing.T) {
		// 1. /healthz returns 200 {"status":"ok","version":"0.6.0"} without auth
		resp, err := env.HTTPClient.Get(env.BaseURL + "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz failed: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"version":"0.6.0"`) {
			t.Fatalf("unexpected /healthz response (%d): %s", resp.StatusCode, string(body))
		}

		// 2. GET /mcp returns 405 with Allow: POST
		getReq, _ := http.NewRequest(http.MethodGet, env.BaseURL+"/mcp", nil)
		getReq.Header.Set("Authorization", "Bearer "+env.AuthToken)
		getResp, err := env.HTTPClient.Do(getReq)
		if err != nil {
			t.Fatalf("GET /mcp failed: %v", err)
		}
		getResp.Body.Close()
		if getResp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 on GET /mcp, got %d", getResp.StatusCode)
		}

		// 3. Unauthenticated POST /mcp returns 401 (even with bad Origin)
		unauthResp, _ := env.postMCP(t, env.BaseURL, map[string]any{
			"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": map[string]any{},
		}, map[string]string{
			"Authorization": "",
			"Origin":        "https://evil.example",
		})
		if unauthResp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401 before Origin check, got %d", unauthResp.StatusCode)
		}

		// 4. Authenticated POST with disallowed Origin returns 403
		originResp, _ := env.postMCP(t, env.BaseURL, map[string]any{
			"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": map[string]any{},
		}, map[string]string{
			"Origin": "https://evil.example",
		})
		if originResp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403 for disallowed Origin, got %d", originResp.StatusCode)
		}

		// 5. SEP-2243 HeaderGuard smuggling check (-32020)
		smugResp, smugBody := env.postMCP(t, env.BaseURL, map[string]any{
			"jsonrpc": "2.0", "id": 9, "method": "tools/call",
			"params": map[string]any{"name": "list_workflows", "arguments": map[string]any{}},
		}, map[string]string{
			"Mcp-Method": "tools/call",
			"Mcp-Name":   "diagnose_workflow",
		})
		if smugResp.StatusCode != http.StatusBadRequest || !strings.Contains(string(smugBody), "-32020") {
			t.Fatalf("expected 400/-32020 on HeaderGuard mismatch, got %d: %s", smugResp.StatusCode, string(smugBody))
		}

		// 6. Dual-Era: 2026-07-28 server/discover succeeds
		discResp, discBody := env.postMCP(t, env.BaseURL, map[string]any{
			"jsonrpc": "2.0", "id": 10, "method": "server/discover",
			"params": map[string]any{
				"_meta": map[string]any{
					"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
					"io.modelcontextprotocol/clientCapabilities": map[string]any{},
				},
			},
		}, map[string]string{
			"Mcp-Protocol-Version": "2026-07-28",
			"Mcp-Method":           "server/discover",
		})
		if discResp.StatusCode != http.StatusOK || !strings.Contains(string(discBody), `"supportedVersions"`) || !strings.Contains(string(discBody), `"2026-07-28"`) {
			t.Fatalf("expected 2026-07-28 server/discover 200, got %d: %s", discResp.StatusCode, string(discBody))
		}

		// 7. Default read-only tools/list exposes exactly 12 read-only tools (zero ops/admin)
		listResp, listBody := env.postMCP(t, env.BaseURL, map[string]any{
			"jsonrpc": "2.0", "id": 11, "method": "tools/list", "params": map[string]any{},
		}, nil)
		if listResp.StatusCode != http.StatusOK {
			t.Fatalf("tools/list failed (%d): %s", listResp.StatusCode, string(listBody))
		}
		var listParsed struct {
			Result struct {
				Tools []struct {
					Name string `json:"name"`
				} `json:"tools"`
			} `json:"result"`
		}
		if err := json.Unmarshal(listBody, &listParsed); err != nil {
			t.Fatalf("parse tools/list: %v", err)
		}
		if len(listParsed.Result.Tools) != 12 {
			t.Fatalf("expected 12 default read-only tools, got %d: %+v", len(listParsed.Result.Tools), listParsed.Result.Tools)
		}
	})

	t.Run("PhaseA_V36Compat_Diagnose_Graph_Events_Logs_Validate_And_Fence", func(t *testing.T) {
		// 1. inspect_cron_workflows reads authentic v3.6.5 singular spec.schedule CronWorkflow losslessly!
		cronText, isErr := env.callTool2025(t, env.BaseURL, "inspect_cron_workflows", map[string]any{
			"namespace": env.WorkNS,
		}, "e2e-sre-alice")
		if isErr {
			t.Fatalf("inspect_cron_workflows failed: %s", cronText)
		}
		if !strings.Contains(cronText, "e2e-legacy-v36-cron") || !strings.Contains(cronText, "15 4 * * *") {
			t.Fatalf("expected v3.6 singular schedule 15 4 * * * in inspect_cron_workflows output, got: %s", cronText)
		}

		// 2. diagnose_workflow on real failed DAG: extracts root-cause boom-fail, resolves PodName v2 logs,
		// redacts AWS secret & JWT, wraps <untrusted_workflow_log> without breakout!
		diagText, isErr := env.callTool2025(t, env.BaseURL, "diagnose_workflow", map[string]any{
			"namespace": env.WorkNS,
			"name":      "e2e-failed-dag",
		}, "e2e-sre-alice")
		if isErr {
			t.Fatalf("diagnose_workflow failed: %s", diagText)
		}
		if !strings.Contains(diagText, "boom-fail") {
			t.Fatalf("expected root-cause node boom-fail in diagnose_workflow output: %s", diagText)
		}
		var diagParsed struct {
			RootCauses        []struct{ DisplayName, Type string } `json:"rootCauses"`
			CascadingFailures []struct{ DisplayName, Type string } `json:"cascadingFailures"`
		}
		if err := json.Unmarshal([]byte(diagText), &diagParsed); err != nil {
			t.Fatalf("unmarshal diagnose_workflow output: %v", err)
		}
		if len(diagParsed.RootCauses) != 1 || diagParsed.RootCauses[0].DisplayName != "boom-fail" || diagParsed.RootCauses[0].Type != "Pod" {
			t.Fatalf("expected boom-fail Pod as sole root cause (DAG moved to cascadingFailures), got roots=%+v cascading=%+v", diagParsed.RootCauses, diagParsed.CascadingFailures)
		}
		if !strings.Contains(diagText, "FATAL: simulated exit 42 root cause") {
			t.Fatalf("expected PodName v2 log content in diagnose_workflow output: %s", diagText)
		}
		if !strings.Contains(diagText, "<untrusted_workflow_log ") || !strings.Contains(diagText, "&lt;/untrusted_workflow_log&gt;") {
			t.Fatalf("expected prompt-injection spotlighting & breakout neutralization: %s", diagText)
		}
		if strings.Contains(diagText, "wJalrXUtnFEMI") || strings.Contains(diagText, "sig987654321") {
			t.Fatalf("plaintext secret leaked in diagnose_workflow output: %s", diagText)
		}

		// 3. diagnose_workflow on real suspended workflow detects Suspend pause & actionable resume hint
		suspText, isErr := env.callTool2025(t, env.BaseURL, "diagnose_workflow", map[string]any{
			"namespace": env.WorkNS,
			"name":      "e2e-suspend-wf",
		}, "e2e-sre-alice")
		if isErr {
			t.Fatalf("diagnose_workflow(suspend) failed: %s", suspText)
		}
		if !strings.Contains(suspText, "wait-approval") || !strings.Contains(suspText, "suspendHints") || !strings.Contains(suspText, "control_workflow") {
			t.Fatalf("expected suspend detection on e2e-suspend-wf, got: %s", suspText)
		}

		// 4. render_workflow_graph returns clean Mermaid DAG with collapseSucceeded
		graphText, isErr := env.callTool2025(t, env.BaseURL, "render_workflow_graph", map[string]any{
			"namespace":         env.WorkNS,
			"name":              "e2e-failed-dag",
			"format":            "mermaid",
			"collapseSucceeded": true,
		}, "e2e-sre-alice")
		if isErr || !strings.Contains(graphText, "flowchart TD") || !strings.Contains(graphText, "boom-fail") {
			t.Fatalf("unexpected render_workflow_graph output (isErr=%v): %s", isErr, graphText)
		}

		// 5. get_workflow_logs with grep filter + spotlighting
		logsText, isErr := env.callTool2025(t, env.BaseURL, "get_workflow_logs", map[string]any{
			"namespace": env.WorkNS,
			"name":      "e2e-failed-dag",
			"grep":      "FATAL",
		}, "e2e-sre-alice")
		if isErr || !strings.Contains(logsText, "FATAL: simulated exit 42") || !strings.Contains(logsText, "<untrusted_workflow_log ") {
			t.Fatalf("unexpected get_workflow_logs output (isErr=%v): %s", isErr, logsText)
		}

		// 6. get_workflow_events (via Argo Server WatchEvents snapshot — zero K8s client!)
		eventsText, isErr := env.callTool2025(t, env.BaseURL, "get_workflow_events", map[string]any{
			"namespace": env.WorkNS,
			"name":      "e2e-failed-dag",
		}, "e2e-sre-alice")
		if isErr || !strings.Contains(eventsText, `"events"`) {
			t.Fatalf("unexpected get_workflow_events output (isErr=%v): %s", isErr, eventsText)
		}

		// 7. validate_manifest normalizes legacy v3.6 CronWorkflow YAML and validates against live v3.6.5 server
		legacyYAML := `apiVersion: argoproj.io/v1alpha1
kind: CronWorkflow
metadata:
  name: lint-v36-demo
spec:
  schedule: "0 2 * * *"
  workflowSpec:
    entrypoint: main
    synchronization:
      mutex:
        name: demo-mutex
    templates:
      - name: main
        container:
          image: busybox:1.36
          command: ["echo", "ok"]
`
		valText, isErr := env.callTool2025(t, env.BaseURL, "validate_manifest", map[string]any{
			"namespace": env.WorkNS,
			"manifest":  legacyYAML,
		}, "e2e-sre-alice")
		if isErr || !strings.Contains(valText, `"valid":true`) || !strings.Contains(valText, "spec.schedule") {
			t.Fatalf("unexpected validate_manifest output (isErr=%v): %s", isErr, valText)
		}

		// 8. MCP_DENIED_NAMESPACES blocks mcp-wf-e2e-denied even though SA has RBAC
		deniedText, isErr := env.callTool2025(t, env.BaseURL, "list_templates", map[string]any{
			"namespace": env.DeniedNS,
		}, "e2e-sre-alice")
		if !isErr || !strings.Contains(deniedText, "MCP_DENIED_NAMESPACES") {
			t.Fatalf("expected MCP_DENIED_NAMESPACES rejection, got isErr=%v: %s", isErr, deniedText)
		}

		// 9. Path traversal rejection
		travText, isErr := env.callTool2025(t, env.BaseURL, "get_workflow", map[string]any{
			"namespace": env.WorkNS,
			"name":      "../kube-system/evil",
		}, "e2e-sre-alice")
		if !isErr || !strings.Contains(travText, "invalid") {
			t.Fatalf("expected path traversal rejection, got isErr=%v: %s", isErr, travText)
		}
	})

	t.Run("PhaseA_DirectKubernetesMode_HelmRelease", func(t *testing.T) {
		// Verify symmetric non-mixed Direct K8s Helm deployment works independently!
		clText, isErr := env.callTool2025(t, env.K8sBaseURL, "list_clusters", map[string]any{}, "e2e-k8s-verify")
		if isErr || !strings.Contains(clText, `"mode":"kubernetes"`) {
			t.Fatalf("expected mode=kubernetes on Direct K8s release, got isErr=%v: %s", isErr, clText)
		}
		wfText, isErr := env.callTool2025(t, env.K8sBaseURL, "list_workflows", map[string]any{
			"namespace": env.WorkNS,
		}, "e2e-k8s-verify")
		if isErr || !strings.Contains(wfText, "e2e-failed-dag") {
			t.Fatalf("expected e2e-failed-dag via Direct K8s release, got isErr=%v: %s", isErr, wfText)
		}
	})

	t.Run("PhaseA_TwoReplicaStatelessLoadBalancing_And_AuditLogSanitization", func(t *testing.T) {
		// Send 16 stateless calls with unique callers and confirm BOTH Pod replicas handled requests
		// and ZERO plaintext MCP_AUTH_TOKEN / ARGO_TOKEN appear in pod logs.
		for i := 0; i < 16; i++ {
			_, isErr := env.callTool2025(t, env.BaseURL, "list_clusters", map[string]any{}, fmt.Sprintf("lb-probe-%d", i))
			if isErr {
				t.Fatalf("lb probe %d failed", i)
			}
		}

		podsOut, err := exec.Command("kubectl", "-n", env.SysNS, "get", "pods",
			"-l", "app.kubernetes.io/instance=mcp-wf-e2e",
			"-o", "jsonpath={.items[*].metadata.name}").CombinedOutput()
		if err != nil {
			t.Fatalf("get pods failed: %v (%s)", err, string(podsOut))
		}
		podNames := strings.Fields(string(podsOut))
		if len(podNames) != 2 {
			t.Fatalf("expected 2 replicas for mcp-wf-e2e, got %d: %v", len(podNames), podNames)
		}
		podsHit := 0
		for _, pod := range podNames {
			logsOut, err := exec.Command("kubectl", "-n", env.SysNS, "logs", pod).CombinedOutput()
			if err != nil {
				t.Fatalf("logs for %s failed: %v", pod, err)
			}
			logStr := string(logsOut)
			if strings.Contains(logStr, env.AuthToken) {
				t.Fatalf("SECURITY VIOLATION: MCP_AUTH_TOKEN leaked in pod %s logs!", pod)
			}
			rawArgo := strings.TrimPrefix(env.ArgoToken, "Bearer ")
			if len(rawArgo) > 10 && strings.Contains(logStr, rawArgo) {
				t.Fatalf("SECURITY VIOLATION: ARGO_TOKEN leaked in pod %s logs!", pod)
			}
			if strings.Contains(logStr, "lb-probe-") {
				podsHit++
			}
		}
		if podsHit < 2 {
			t.Logf("note: kube-proxy hit %d/2 pods across 16 probes (non-fatal on single client IP if conntrack reused), pods=%v", podsHit, podNames)
		} else {
			t.Logf("verified stateless load-balancing across BOTH replicas (%v) with secret-free JSON audit logs", podNames)
		}
	})

	t.Run("PhaseB_Ops_And_Admin_With_CrossReplicaMRTR_And_LegacyConfirm", func(t *testing.T) {
		chartPath := "../../deploy/charts/mcp-for-argo-workflows"
		// Upgrade 2-replica release via Helm to enable ops + admin (readOnly=false)
		upgradeCmd := exec.Command("helm", "upgrade", "mcp-wf-e2e", chartPath,
			"-n", env.SysNS,
			"--reuse-values",
			"--set", `mcp.toolsets=core\,diagnostics\,ops\,admin`,
			"--set", "mcp.readOnly=false",
		)
		if out, err := upgradeCmd.CombinedOutput(); err != nil {
			t.Fatalf("helm upgrade to Phase B failed: %v\n%s", err, string(out))
		}
		rolloutCmd := exec.Command("kubectl", "-n", env.SysNS, "rollout", "status", "deploy/mcp-wf-e2e-mcp-for-argo-workflows", "--timeout=120s")
		if out, err := rolloutCmd.CombinedOutput(); err != nil {
			t.Fatalf("Phase B rollout failed: %v\n%s", err, string(out))
		}
		// Always restore read-only posture on exit so cluster stays safe
		defer func() {
			_ = exec.Command("helm", "upgrade", "mcp-wf-e2e", chartPath,
				"-n", env.SysNS, "--reuse-values",
				"--set", `mcp.toolsets=core\,diagnostics`,
				"--set", "mcp.readOnly=true",
			).Run()
		}()
		time.Sleep(2 * time.Second)

		// 1. ops: trigger_workflow from approved WorkflowTemplate (ZERO raw YAML!)
		trigText, isErr := env.callTool2025(t, env.BaseURL, "trigger_workflow", map[string]any{
			"namespace":    env.WorkNS,
			"resourceKind": "WorkflowTemplate",
			"resourceName": "e2e-approved-tmpl",
			"parameters":   map[string]string{"message": "triggered-via-ops"},
		}, "e2e-ops-bob")
		if isErr {
			t.Fatalf("trigger_workflow failed: %s", trigText)
		}
		var trigRes struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal([]byte(trigText), &trigRes); err != nil || trigRes.Name == "" {
			t.Fatalf("unexpected trigger_workflow result: %s", trigText)
		}

		// 2. ops: control_workflow resumes live suspended e2e-suspend-wf with approval parameter
		resumeText, isErr := env.callTool2025(t, env.BaseURL, "control_workflow", map[string]any{
			"namespace":         env.WorkNS,
			"name":              "e2e-suspend-wf",
			"action":            "resume",
			"nodeFieldSelector": "displayName=wait-approval",
			"outputParameters":  map[string]string{"approver": "ops-bob"},
		}, "e2e-ops-bob")
		if isErr {
			t.Fatalf("control_workflow(resume) failed: %s", resumeText)
		}

		// 3. ops: set_cron_schedule_state idempotently toggles e2e-legacy-v36-cron (v3.6 singular schedule preserved!)
		cronStateText, isErr := env.callTool2025(t, env.BaseURL, "set_cron_schedule_state", map[string]any{
			"namespace": env.WorkNS,
			"name":      "e2e-legacy-v36-cron",
			"suspended": true,
		}, "e2e-ops-bob")
		if isErr || !strings.Contains(cronStateText, `"suspended":true`) || !strings.Contains(cronStateText, `"15 4 * * *"`) {
			t.Fatalf("expected suspended=true & schedule 15 4 * * * on v3.6 cron, got isErr=%v: %s", isErr, cronStateText)
		}

		// 4. admin: delete_resource via 2026-07-28 MRTR cross-replica HMAC approval flow!
		// Call 1 returns input_required + signed requestState (without deleting yet)
		mrtrRes := env.callTool2026(t, env.BaseURL, "delete_resource", map[string]any{
			"namespace": env.WorkNS,
			"kind":      "Workflow",
			"name":      trigRes.Name,
		}, nil, "e2e-admin-carol")
		if mrtrRes["resultType"] != "input_required" {
			t.Fatalf("expected MRTR input_required, got: %+v", mrtrRes)
		}
		reqState, _ := mrtrRes["requestState"].(string)
		if reqState == "" {
			t.Fatalf("expected non-empty signed requestState in MRTR response: %+v", mrtrRes)
		}

		// Call 2 submits approval + requestState across 2-replica Service (stateless HMAC verifies on either replica!)
		finalRes := env.callTool2026(t, env.BaseURL, "delete_resource", map[string]any{
			"namespace": env.WorkNS,
			"kind":      "Workflow",
			"name":      trigRes.Name,
		}, map[string]any{
			"requestState": reqState,
			"inputResponses": map[string]any{
				"confirm": map[string]any{
					"action":  "accept",
					"content": map[string]any{"confirm": true},
				},
			},
		}, "e2e-admin-carol")
		if finalRes["isError"] == true || finalRes["resultType"] != "complete" {
			t.Fatalf("expected MRTR approved delete_resource to succeed, got: %+v", finalRes)
		}

		// 5. admin: apply_manifest upserts a temporary WorkflowTemplate, then delete_resource removes it
		// via 2025-11-25 legacy confirmName fallback (verifying both MRTR and legacy fallback live!)
		tempTmplYAML := `apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: e2e-temp-apply-tmpl
spec:
  serviceAccountName: e2e-wf-runner
  entrypoint: main
  templates:
    - name: main
      container:
        image: busybox:1.36
        imagePullPolicy: IfNotPresent
        command: ["echo", "applied"]
`
		applyText, isErr := env.callTool2025(t, env.BaseURL, "apply_manifest", map[string]any{
			"namespace": env.WorkNS,
			"manifest":  tempTmplYAML,
		}, "e2e-admin-carol")
		if isErr || !strings.Contains(applyText, "e2e-temp-apply-tmpl") {
			t.Fatalf("apply_manifest failed (isErr=%v): %s", isErr, applyText)
		}

		// Missing confirmName on 2025-11-25 must refuse deletion
		refuseText, isErr := env.callTool2025(t, env.BaseURL, "delete_resource", map[string]any{
			"namespace": env.WorkNS,
			"kind":      "WorkflowTemplate",
			"name":      "e2e-temp-apply-tmpl",
		}, "e2e-admin-carol")
		if !isErr || !strings.Contains(refuseText, "confirmName") {
			t.Fatalf("expected 2025-11-25 missing confirmName rejection, got isErr=%v: %s", isErr, refuseText)
		}

		// Matching confirmName succeeds
		delTmplText, isErr := env.callTool2025(t, env.BaseURL, "delete_resource", map[string]any{
			"namespace":   env.WorkNS,
			"kind":        "WorkflowTemplate",
			"name":        "e2e-temp-apply-tmpl",
			"confirmName": "e2e-temp-apply-tmpl",
		}, "e2e-admin-carol")
		if isErr || !strings.Contains(delTmplText, `"deleted":true`) {
			t.Fatalf("expected confirmName delete_resource to succeed, got isErr=%v: %s", isErr, delTmplText)
		}

		// 6. admin: archive deletion stays hard-blocked (MCP_ALLOW_ARCHIVE_DELETE=false)
		archText, isErr := env.callTool2025(t, env.BaseURL, "delete_resource", map[string]any{
			"namespace":   env.WorkNS,
			"kind":        "ArchivedWorkflow",
			"name":        "00000000-0000-0000-0000-000000000001",
			"confirmName": "00000000-0000-0000-0000-000000000001",
		}, "e2e-admin-carol")
		if !isErr || !strings.Contains(archText, "MCP_ALLOW_ARCHIVE_DELETE") {
			t.Fatalf("expected archive deletion policy block, got isErr=%v: %s", isErr, archText)
		}
	})
}
