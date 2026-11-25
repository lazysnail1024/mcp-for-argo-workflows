# 本地客户端连接 Argo Workflows MCP 服务配置指南

本文档面向研发与运维同学（支持 Windows / macOS / Linux），说明如何在 **Cursor、VS Code、Claude Code、Windsurf、Cline、Antigravity** 等主流 AI 客户端中接入部署在华为云 CCE 的 **Argo Workflows MCP 服务**（远程 Streamable HTTP）或在本地以 `stdio` 模式运行。

- **默认只读模式**：提供 **12 个高信噪比语义工具**（核心巡检 `core` + 深度排障 `diagnostics`），支持一键聚合排障、PodName v2 日志自动解析、v3.6+ ~ v4.x CronWorkflow 巡检、Mermaid/ASCII 拓扑图渲染与 5 合 1 YAML 预检。
- **纯无状态 Streamable HTTP**：切勿选择旧版 `SSE`（已彻底移除有状态 `/sse` 端点）。

> 服务端（华为云 CCE + Istio + Helm Chart）部署与运维手册请见：[huawei-cce-deployment-guide.md](./huawei-cce-deployment-guide.md)。

---

## 一、 接入前向运维获取的信息

| 参数 | 说明 | 示例值 |
| :--- | :--- | :--- |
| **`MCP_URL`** | Argo Workflows MCP 的 HTTPS 接入地址 | `https://<你的公网域名>/mcp/argo-workflows` |
| **`MCP_AUTH_TOKEN`** | 入站 Bearer 鉴权密钥（非 K8s / Argo 登录凭据） | `a1b2c3...`（64 位字符串） |
| **`X-MCP-Caller`**（推荐） | 你的英文名或工号，写入服务端审计日志便于排障溯源 | `zhangsan` |

> **安全提醒**：请勿将包含真实 `MCP_AUTH_TOKEN` 的配置文件提交到 Git 仓库！优先写在用户全局目录或使用 IDE 密码输入框 / 环境变量读取。

---

## 二、 1 分钟连通性自检

### macOS / Linux

```bash
curl -fsS -X POST "https://<你的公网域名>/mcp/argo-workflows" \
  -H "Authorization: Bearer <MCP_AUTH_TOKEN>" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "X-MCP-Caller: zhangsan" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

### Windows PowerShell

```powershell
$McpUrl = "https://<你的公网域名>/mcp/argo-workflows"
$Token  = "<替换为你的 MCP_AUTH_TOKEN>"
$Headers = @{
  "Authorization" = "Bearer $Token"
  "Accept"        = "application/json, text/event-stream"
  "X-MCP-Caller"  = "zhangsan"
}

$Resp = Invoke-RestMethod -Uri $McpUrl -Method Post -Headers $Headers `
  -ContentType "application/json" `
  -Body '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
$Resp.result.tools | Select-Object name
```

看到返回 12 个默认只读工具列表，即表示网络、证书与鉴权链路完全畅通。

---

## 三、 主流客户端配置模板（远程 Streamable HTTP）

### 3.1 Cursor

编辑 `~/.cursor/mcp.json`（Windows 为 `%USERPROFILE%\.cursor\mcp.json`）：

```json
{
  "mcpServers": {
    "argo-workflows-cce": {
      "url": "https://<你的公网域名>/mcp/argo-workflows",
      "headers": {
        "Authorization": "Bearer <MCP_AUTH_TOKEN>",
        "X-MCP-Caller": "zhangsan"
      }
    }
  }
}
```

### 3.2 VS Code（原生 MCP）

运行 `MCP: Open User Configuration`：

```json
{
  "inputs": [
    {
      "id": "argo_workflows_mcp_token",
      "type": "promptString",
      "description": "输入 Argo Workflows MCP 的入站 MCP_AUTH_TOKEN",
      "password": true
    }
  ],
  "servers": {
    "argo-workflows-cce": {
      "type": "http",
      "url": "https://<你的公网域名>/mcp/argo-workflows",
      "headers": {
        "Authorization": "Bearer ${input:argo_workflows_mcp_token}",
        "X-MCP-Caller": "zhangsan"
      }
    }
  }
}
```

### 3.3 Claude Code（CLI）

```bash
claude mcp add --scope user --transport http argo-workflows-cce https://<你的公网域名>/mcp/argo-workflows \
  --header "Authorization: Bearer <MCP_AUTH_TOKEN>" \
  --header "X-MCP-Caller: zhangsan"
```

### 3.4 Windsurf / Cline / Antigravity

- **Windsurf / Antigravity**（`mcp_config.json`）：
  ```json
  {
    "mcpServers": {
      "argo-workflows-cce": {
        "serverUrl": "https://<你的公网域名>/mcp/argo-workflows",
        "headers": {
          "Authorization": "Bearer <MCP_AUTH_TOKEN>",
          "X-MCP-Caller": "zhangsan"
        }
      }
    }
  }
  ```
- **Cline / Roo Code**（`cline_mcp_settings.json`）：
  ```json
  {
    "mcpServers": {
      "argo-workflows-cce": {
        "type": "streamableHttp",
        "url": "https://<你的公网域名>/mcp/argo-workflows",
        "headers": {
          "Authorization": "Bearer <MCP_AUTH_TOKEN>",
          "X-MCP-Caller": "zhangsan"
        }
      }
    }
  }
  ```

---

## 四、 本地单机 `stdio` 模式配置（可选）

适合持有本机 kubeconfig 或 Argo Server Token 的开发者直接在本地启动二进制：

```json
{
  "mcpServers": {
    "argo-workflows-local": {
      "command": "/usr/local/bin/mcp-for-argo-workflows",
      "args": ["stdio", "--namespace", "argo"],
      "env": {
        "ARGO_SERVER": "https://localhost:2746",
        "ARGO_TOKEN": "Bearer <YOUR_ARGO_TOKEN>",
        "MCP_TOOLSETS": "core,diagnostics"
      }
    }
  }
}
```

> 若本地不连 `argo-server` 而直接读取 `~/.kube/config`，移除 `ARGO_SERVER` / `ARGO_TOKEN` 即可自动进入纯 Direct Kubernetes 模式。

---

## 五、 常用排障提问示例

默认 12 个只读工具：
- **`core`（7）**：`list_clusters`、`list_workflows`、`get_workflow`、`diagnose_workflow`、`list_templates`、`get_template`、`inspect_cron_workflows`
- **`diagnostics`（5）**：`get_workflow_node`、`get_workflow_logs`、`get_workflow_events`、`render_workflow_graph`、`validate_manifest`

推荐在 AI 对话框中直接输入：
1. **一键定位失败根因**：「用 `diagnose_workflow` 诊断命名空间 `argo` 里失败的 Workflow `<name>`，找出真正的根因叶子节点、退出码、关键日志和 Warning 事件。」
2. **查看定时任务健康度**：「调用 `inspect_cron_workflows` 检查 `argo` 命名空间里有哪些 CronWorkflow 处于暂停、并发阻塞或最近执行失败状态。」
3. **可视化 DAG 拓扑**：「用 `render_workflow_graph` 生成 `<workflow-name>` 的 Mermaid 拓扑图，折叠成功的节点重点展示失败链路。」
4. **提交前校验 v3.6 YAML**：「用 `validate_manifest` 校验下面这段 CronWorkflow YAML，检查是否有 v3.6 旧字段需要升级或潜在语法错误。」

---

## 六、 常见故障排查

| 报错 / 现象 | 常见原因 | 排查与解决方法 |
| :--- | :--- | :--- |
| **`401 Missing or invalid inbound bearer token`** | 请求头缺少 `Bearer ` 前缀或 Token 过期/多出空格 | 核对 `Authorization: Bearer <MCP_AUTH_TOKEN>` 格式。 |
| **`403 Forbidden` / `Invalid Host or Origin`** | 不在公司出口 IP 白名单，或浏览器类客户端发送了未放行的 `Origin` | 切换回办公网；若为 MCP Inspector 等浏览器客户端，联系运维将对应 Origin 加入 `mcp.allowedOrigins`。 |
| **`400` / JSON-RPC `-32020`** | 中间网关改写了 `Mcp-Method` / `Mcp-Name` / `Mcp-Param-*` 路由头，与 JSON-RPC Body 不一致 | 检查代理规则，不要伪造或覆写 `Mcp-*` 协议头。 |
| **`405 Method Not Allowed`** | 客户端配置成了已移除的 `SSE` 传输类型（发起了 `GET`） | 切换为 **Streamable HTTP (`http`)**。注：部分 2025 客户端握手时探测 `GET` 收到 `405` 后会自动回退 `POST`，不影响正常使用。 |
| **响应提示超过 `64KB` 熔断限制** | 一次拉取了过大的 Workflow 全量节点或过多日志 | 按错误提示改用 `diagnose_workflow`、`get_workflow(detailLevel="abnormal")`、`get_workflow_node` 或收紧 `limit`/`grep` 参数。 |
