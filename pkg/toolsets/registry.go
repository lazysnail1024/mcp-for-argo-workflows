package toolsets

import (
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// AllToolMetas returns the complete catalog of 18 semantic tools across the 4 toolsets.
func AllToolMetas() []ToolMeta {
	metas := []ToolMeta{
		// core (7 read-only tools, enabled by default)
		{Name: "diagnose_workflow", Toolset: ToolsetCore, ReadOnly: true, Register: registerDiagnoseWorkflow},
		{Name: "get_template", Toolset: ToolsetCore, ReadOnly: true, Register: registerGetTemplate},
		{Name: "get_workflow", Toolset: ToolsetCore, ReadOnly: true, Register: registerGetWorkflow},
		{Name: "inspect_cron_workflows", Toolset: ToolsetCore, ReadOnly: true, Register: registerInspectCronWorkflows},
		{Name: "list_clusters", Toolset: ToolsetCore, ReadOnly: true, Register: registerListClusters},
		{Name: "list_templates", Toolset: ToolsetCore, ReadOnly: true, Register: registerListTemplates},
		{Name: "list_workflows", Toolset: ToolsetCore, ReadOnly: true, Register: registerListWorkflows},

		// diagnostics (5 read-only deep inspection tools, enabled by default)
		{Name: "get_workflow_events", Toolset: ToolsetDiagnostics, ReadOnly: true, Register: registerGetWorkflowEvents},
		{Name: "get_workflow_logs", Toolset: ToolsetDiagnostics, ReadOnly: true, Register: registerGetWorkflowLogs},
		{Name: "get_workflow_node", Toolset: ToolsetDiagnostics, ReadOnly: true, Register: registerGetWorkflowNode},
		{Name: "render_workflow_graph", Toolset: ToolsetDiagnostics, ReadOnly: true, Register: registerRenderWorkflowGraph},
		{Name: "validate_manifest", Toolset: ToolsetDiagnostics, ReadOnly: true, Register: registerValidateManifest},

		// ops (4 controlled day-2 operational tools, disabled by default; zero raw container YAML execution)
		{Name: "control_workflow", Toolset: ToolsetOps, ReadOnly: false, Register: registerControlWorkflow},
		{Name: "recover_workflow", Toolset: ToolsetOps, ReadOnly: false, Register: registerRecoverWorkflow},
		{Name: "set_cron_schedule_state", Toolset: ToolsetOps, ReadOnly: false, Register: registerSetCronScheduleState},
		{Name: "trigger_workflow", Toolset: ToolsetOps, ReadOnly: false, Register: registerTriggerWorkflow},

		// admin (2 high-privilege definition & destruction tools, disabled by default)
		{Name: "apply_manifest", Toolset: ToolsetAdmin, ReadOnly: false, Register: registerApplyManifest},
		{Name: "delete_resource", Toolset: ToolsetAdmin, ReadOnly: false, Register: registerDeleteResource},
	}

	// Deterministic lexicographical ordering stabilizes tools/list caching and prompt caching.
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Name < metas[j].Name
	})
	return metas
}

// ResolveEnabledTools validates toolset/tool names, applies MCP_READ_ONLY precedence,
// and returns the sorted list of tools to register.
func ResolveEnabledTools(sel SelectionConfig) ([]ToolMeta, error) {
	enabledSets, err := parseEnabledToolsets(sel.Toolsets)
	if err != nil {
		return nil, err
	}

	// MCP_READ_ONLY=true unconditionally strips ops and admin regardless of --toolsets.
	if sel.ReadOnly {
		delete(enabledSets, ToolsetOps)
		delete(enabledSets, ToolsetAdmin)
	}

	allMetas := AllToolMetas()
	knownTools := make(map[string]ToolMeta, len(allMetas))
	for _, m := range allMetas {
		knownTools[m.Name] = m
	}

	var toolAllow map[string]bool
	if len(sel.Tools) > 0 {
		toolAllow = make(map[string]bool, len(sel.Tools))
		for _, raw := range sel.Tools {
			for _, part := range strings.Split(raw, ",") {
				name := strings.TrimSpace(part)
				if name == "" {
					continue
				}
				if _, ok := knownTools[name]; !ok {
					return nil, fmt.Errorf("unknown tool name %q in --tools / MCP_TOOLS", name)
				}
				toolAllow[name] = true
			}
		}
	}

	selected := make([]ToolMeta, 0, len(allMetas))
	for _, m := range allMetas {
		if !enabledSets[m.Toolset] {
			continue
		}
		if sel.ReadOnly && !m.ReadOnly {
			continue
		}
		if len(toolAllow) > 0 && !toolAllow[m.Name] {
			continue
		}
		selected = append(selected, m)
	}
	return selected, nil
}

// RegisterEnabled registers all tools authorized by sel in deterministic alphabetical order.
func RegisterEnabled(s *mcp.Server, deps Dependencies, sel SelectionConfig) ([]string, error) {
	enabled, err := ResolveEnabledTools(sel)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(enabled))
	for _, m := range enabled {
		m.Register(s, deps)
		names = append(names, m.Name)
	}
	return names, nil
}

func parseEnabledToolsets(rawList []string) (map[ToolsetName]bool, error) {
	if len(rawList) == 0 {
		return map[ToolsetName]bool{
			ToolsetCore:        true,
			ToolsetDiagnostics: true,
		}, nil
	}

	enabled := make(map[ToolsetName]bool)
	for _, raw := range rawList {
		for _, part := range strings.Split(raw, ",") {
			token := strings.ToLower(strings.TrimSpace(part))
			if token == "" {
				continue
			}
			switch token {
			case "all":
				for _, ts := range AllToolsetNames {
					enabled[ts] = true
				}
			case string(ToolsetCore):
				enabled[ToolsetCore] = true
			case string(ToolsetDiagnostics):
				enabled[ToolsetDiagnostics] = true
			case string(ToolsetOps):
				enabled[ToolsetOps] = true
			case string(ToolsetAdmin):
				enabled[ToolsetAdmin] = true
			default:
				return nil, fmt.Errorf("unknown toolset %q: valid toolsets are core, diagnostics, ops, admin, all", token)
			}
		}
	}
	return enabled, nil
}
