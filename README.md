# mcp-for-argo-workflows

Production-grade **Argo Workflows MCP Server** (`v0.6.0`) built in Go on the official `github.com/modelcontextprotocol/go-sdk` (`v1.7.0`), engineered for **Argo Workflows v3.6+ ~ v4.x** compatibility, **intent-driven high-signal diagnostics**, and **stateless multi-replica Kubernetes / Huawei CCE** deployment.

> **Breaking overhaul in `v0.6.0`**: Replaces 39 legacy CLI-mirroring tools, prompts, resources, `go-graphviz`, and dual-endpoint `/sse` with **18 curated semantic tools across 4 gated toolsets**, pure stateless Streamable HTTP (`/mcp`) + `stdio`, Dual-Era (`2026-07-28` & `2025-11-25`) protocol support, and hardened enterprise governance.

## Key Capabilities

- **Argo Workflows `v3.6+ ~ v4.x` Compatibility**:
  - Defaults Argo Server mode to REST JSON (`--argo-http1=true`) with an automatic `NormalizingRoundTripper` that losslessly upgrades legacy `v3.6` singular fields (`spec.schedule`, `synchronization.mutex`, `synchronization.semaphore`) on read and normalizes inbound manifests before validation/submission.
  - Transparently decompresses `status.compressedNodes`, resolves `PodName v2` (`<wf>-<template>-<fnv32a>`), and automatically falls back across `init` / `main` / `wait` containers when fetching failed step logs.
- **18 Semantic Tools in 4 Layered Toolsets** (Default: `core,diagnostics` — 12 read-only tools):
  - **`core` (7, default ON, read-only)**: `list_clusters`, `list_workflows`, `get_workflow`, `diagnose_workflow`, `list_templates`, `get_template`, `inspect_cron_workflows`.
  - **`diagnostics` (5, default ON, read-only)**: `get_workflow_node`, `get_workflow_logs`, `get_workflow_events`, `render_workflow_graph` (Mermaid/ASCII with `collapseSucceeded`), `validate_manifest` (5-in-1 lint + v3.6 normalization).
  - **`ops` (4, default OFF, controlled Day-2 writes)**: `trigger_workflow` (strictly from approved templates/CronWorkflows — **zero arbitrary container YAML**), `recover_workflow` (unified live & archive retry/resubmit), `control_workflow` (`suspend`/`resume` with approval `outputParameters`, `stop`, MRTR-gated `terminate`), `set_cron_schedule_state`.
  - **`admin` (2, default OFF, high-privilege writes)**: `apply_manifest` (raw Workflow submission & Template/Cron upsert), `delete_resource` (MRTR / `confirmName` gated; `MCP_ALLOW_ARCHIVE_DELETE=false` and `MCP_ALLOW_CLUSTER_TEMPLATES=false` by default).
- **Symmetric Non-Mixed Connection Modes**:
  - **Pure Argo Server mode** (`--argo-server` / `ARGO_SERVER`): Recommended for production (supports Archive, reverse-proxy subpaths, custom CA via `--argo-ca-cert`/`SSL_CERT_FILE`, hot-reloaded host-bound `ARGO_TOKEN_REGISTRY_PATH`, and `automountServiceAccountToken=false`).
  - **Pure Direct Kubernetes mode** (empty `--argo-server`): Connects directly via kubeconfig / in-cluster ServiceAccount with explicit `--allowed-contexts` guard when serving HTTP.
- **Enterprise Security & Governance**:
  - Loopback default bind (`127.0.0.1:8080`); fail-closed `MCP_AUTH_TOKEN` on exposed binds (verified before Host/Origin checks); `Host`/`Origin` DNS-rebinding guard; SEP-2243 `HeaderGuard` (`-32020`).
  - Cross-replica stateless HMAC-SHA256 MRTR approval gate (`2026-07-28`) with `confirmName` fallback (`2025-11-25`).
  - Automatic secret redaction, `<untrusted_workflow_log>` prompt-injection spotlighting, 64KB hard response byte-breaker, namespace scope fence (`MCP_ALLOWED_NAMESPACES` / `MCP_DENIED_NAMESPACES`), and single-line secret-free JSON audit logs (`X-MCP-Caller`, `traceparent`).
- **Static Distroless Image**: `CGO_ENABLED=0` static Go binary on `gcr.io/distroless/static-debian12:nonroot` (`UID 1000:1000`), default tag `ghcr.io/lazysnail1024/mcp-for-argo-workflows:0.6.0`.

## Quick Start

### 1. Local `stdio` Mode

```bash
# Build static binary (Go 1.26.5+)
make build

# Argo Server mode (v3.6+ / v4.x via REST JSON)
ARGO_SERVER=https://localhost:2746 \
ARGO_TOKEN="Bearer <token>" \
./dist/mcp-for-argo-workflows stdio --namespace argo

# Or Direct Kubernetes mode (uses ~/.kube/config)
./dist/mcp-for-argo-workflows stdio --namespace argo
```

### 2. Stateless Streamable HTTP Mode (`/mcp`)

```bash
MCP_AUTH_TOKEN="$(openssl rand -hex 32)" \
MCP_REQUEST_STATE_SECRET="$(openssl rand -hex 32)" \
ARGO_SERVER=https://argo-server.argo.svc.cluster.local:2746 \
ARGO_TOKEN="Bearer <token>" \
./dist/mcp-for-argo-workflows http \
  --bind-address 0.0.0.0 \
  --port 8080 \
  --allowed-host-header devops.example.com
```

### 3. Production Kubernetes / Huawei CCE Deployment (Helm Chart)

- Companion Helm Chart: [deploy/charts/mcp-for-argo-workflows](deploy/charts/mcp-for-argo-workflows/)
- Huawei CCE + Istio deployment guide: [doc/huawei-cce-deployment-guide.md](doc/huawei-cce-deployment-guide.md)
- End-user MCP client connection guide: [doc/mcp-client-connect-guide.md](doc/mcp-client-connect-guide.md)

```bash
helm upgrade --install mcp-argo-workflows deploy/charts/mcp-for-argo-workflows \
  -n argo \
  -f deploy/charts/mcp-for-argo-workflows/examples/values-cce-istio.yaml
```

## Verification & Development

```bash
make vet            # Static analysis
make test           # Tier 1 unit + Tier 2 Dual-Era stateless protocol integration tests (-race)
make helm-lint      # Lint & template-validate Helm chart and example values
make test-image     # Tier 3 build distroless image & run black-box container smoke suite
make test-e2e-kind  # Tier 4 2-replica Helm E2E against live kind + Argo Workflows v3.6.5
```
