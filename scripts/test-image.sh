#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-ghcr.io/lazysnail1024/mcp-for-argo-workflows:0.6.0}"

echo "==> [Smoke 1] Verify non-root UID 1000:1000 and distroless runtime (zero shell)"
IMG_USER="$(docker inspect --format '{{.Config.User}}' "${IMAGE}")"
if [[ "${IMG_USER}" != "1000:1000" && "${IMG_USER}" != "1000" ]]; then
  echo "FAIL: expected Config.User 1000:1000, got '${IMG_USER}'" >&2
  exit 1
fi
set +e
docker run --rm --entrypoint /bin/sh "${IMAGE}" -c "id" >/dev/null 2>&1
SH_CODE=$?
set -e
if [[ "${SH_CODE}" -eq 0 ]]; then
  echo "FAIL: expected distroless runtime without /bin/sh" >&2
  exit 1
fi
IMG_SIZE="$(docker inspect --format '{{.Size}}' "${IMAGE}")"
echo "PASS: user=${IMG_USER}, distroless (no /bin/sh), size=$((IMG_SIZE / 1024 / 1024))MB"

echo "==> [Smoke 2] Verify fail-closed when binding 0.0.0.0 without MCP_AUTH_TOKEN"
set +e
FAIL_OUT="$(docker run --rm \
  -e MCP_BIND_ADDRESS=0.0.0.0 \
  -e ARGO_SERVER=http://127.0.0.1:2746 \
  "${IMAGE}" http 2>&1)"
FAIL_CODE=$?
set -e
if [[ "${FAIL_CODE}" -eq 0 ]]; then
  echo "FAIL: container should have exited non-zero without MCP_AUTH_TOKEN" >&2
  exit 1
fi
echo "${FAIL_OUT}" | grep -qi "MCP_AUTH_TOKEN"
echo "PASS: fail-closed exit code=${FAIL_CODE}"

echo "==> [Smoke 3] Verify removed 'sse' subcommand exits non-zero"
set +e
SSE_OUT="$(docker run --rm "${IMAGE}" sse 2>&1)"
SSE_CODE=$?
set -e
if [[ "${SSE_CODE}" -eq 0 ]]; then
  echo "FAIL: removed sse subcommand should fail" >&2
  exit 1
fi
echo "${SSE_OUT}" | grep -qi "removed"
echo "PASS: sse subcommand rejected (exit code=${SSE_CODE})"

echo "==> [Smoke 4] Verify 'http', 'http --stateless --json-response', and '--stateless=false' fallback + /healthz + /mcp auth & tools/list"
for MODE_LABEL in "default-http" "explicit-stateless-json" "stateless-false-warning"; do
  CID="argo-wf-mcp-smoke-${MODE_LABEL}-$$"
  if [[ "${MODE_LABEL}" == "default-http" ]]; then
    ARGS=("http")
  elif [[ "${MODE_LABEL}" == "explicit-stateless-json" ]]; then
    ARGS=("http" "--stateless" "--json-response")
  else
    ARGS=("http" "--stateless=false")
  fi

  docker run -d --rm --name "${CID}" \
    -p 127.0.0.1::8080 \
    -e MCP_BIND_ADDRESS=0.0.0.0 \
    -e MCP_AUTH_TOKEN=smoke-bearer-token-060 \
    -e ARGO_SERVER=http://127.0.0.1:2746 \
    "${IMAGE}" "${ARGS[@]}" >/dev/null

  HOST_PORT="$(docker port "${CID}" 8080/tcp | head -n 1 | awk -F: '{print $NF}')"
  OK=0
  for _ in $(seq 1 25); do
    BODY="$(curl -fsS "http://127.0.0.1:${HOST_PORT}/healthz" 2>/dev/null || true)"
    if echo "${BODY}" | grep -q '"status":"ok"' && echo "${BODY}" | grep -q '"version":"0.6.0"'; then
      OK=1
      break
    fi
    sleep 0.2
  done

  if [[ "${OK}" -ne 1 ]]; then
    echo "FAIL: ${MODE_LABEL} did not answer healthy /healthz. Logs:" >&2
    docker logs "${CID}" 2>&1 || true
    docker stop "${CID}" >/dev/null 2>&1 || true
    exit 1
  fi

  UNAUTH_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:${HOST_PORT}/mcp" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}')"
  if [[ "${UNAUTH_CODE}" != "401" ]]; then
    echo "FAIL: ${MODE_LABEL} expected 401 without token, got ${UNAUTH_CODE}" >&2
    docker stop "${CID}" >/dev/null 2>&1 || true
    exit 1
  fi

  GET_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X GET "http://127.0.0.1:${HOST_PORT}/mcp" \
    -H "Authorization: Bearer smoke-bearer-token-060")"
  if [[ "${GET_CODE}" != "405" ]]; then
    echo "FAIL: ${MODE_LABEL} expected 405 on GET /mcp, got ${GET_CODE}" >&2
    docker stop "${CID}" >/dev/null 2>&1 || true
    exit 1
  fi

  TOOLS_JSON="$(curl -fsS -X POST "http://127.0.0.1:${HOST_PORT}/mcp" \
    -H "Authorization: Bearer smoke-bearer-token-060" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json, text/event-stream" \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}')"
  echo "${TOOLS_JSON}" | grep -q '"diagnose_workflow"'
  echo "${TOOLS_JSON}" | grep -q '"inspect_cron_workflows"'
  if echo "${TOOLS_JSON}" | grep -q '"delete_resource"'; then
    echo "FAIL: ${MODE_LABEL} exposed admin tool delete_resource in default read-only mode" >&2
    docker stop "${CID}" >/dev/null 2>&1 || true
    exit 1
  fi

  LOGS="$(docker logs "${CID}" 2>&1 || true)"
  docker stop "${CID}" >/dev/null 2>&1 || true

  if [[ "${MODE_LABEL}" == "stateless-false-warning" ]]; then
    echo "${LOGS}" | grep -q "stateless=false is deprecated"
  fi
  echo "PASS: ${MODE_LABEL} healthy on /healthz, 401 unauth, 405 GET, 12 read-only tools via POST /mcp"
done

echo "ALL TIER 3 CONTAINER SMOKE TESTS PASSED for ${IMAGE}!"
