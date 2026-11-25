package toolsets

import (
	"sort"
	"testing"
)

func TestAllToolMetas_CountAndDeterministicOrder(t *testing.T) {
	metas := AllToolMetas()
	if len(metas) != 18 {
		t.Fatalf("expected exactly 18 semantic tools, got %d", len(metas))
	}

	names := make([]string, len(metas))
	counts := map[ToolsetName]int{}
	for i, m := range metas {
		names[i] = m.Name
		counts[m.Toolset]++
		if m.Register == nil {
			t.Fatalf("tool %q has nil Register handler", m.Name)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("AllToolMetas must be sorted lexicographically, got %v", names)
	}
	if counts[ToolsetCore] != 7 || counts[ToolsetDiagnostics] != 5 || counts[ToolsetOps] != 4 || counts[ToolsetAdmin] != 2 {
		t.Fatalf("unexpected toolset distribution: %+v", counts)
	}
}

func TestResolveEnabledTools_DefaultsAndPrecedence(t *testing.T) {
	// Default: core + diagnostics (12 read-only tools)
	def, err := ResolveEnabledTools(SelectionConfig{})
	if err != nil {
		t.Fatalf("default ResolveEnabledTools failed: %v", err)
	}
	if len(def) != 12 {
		t.Fatalf("expected 12 default tools, got %d", len(def))
	}
	for _, m := range def {
		if !m.ReadOnly {
			t.Fatalf("default selection must only contain read-only tools, found %q (%s)", m.Name, m.Toolset)
		}
	}

	// All toolsets enabled without read-only
	all, err := ResolveEnabledTools(SelectionConfig{Toolsets: []string{"all"}, ReadOnly: false})
	if err != nil {
		t.Fatalf("all toolsets failed: %v", err)
	}
	if len(all) != 18 {
		t.Fatalf("expected 18 tools for all, got %d", len(all))
	}

	// MCP_READ_ONLY=true overrides --toolsets=all
	roOverride, err := ResolveEnabledTools(SelectionConfig{Toolsets: []string{"all"}, ReadOnly: true})
	if err != nil {
		t.Fatalf("read-only override failed: %v", err)
	}
	if len(roOverride) != 12 {
		t.Fatalf("expected 12 tools when ReadOnly overrides all, got %d", len(roOverride))
	}

	// --tools fine-grained allowlist within enabled toolsets
	filtered, err := ResolveEnabledTools(SelectionConfig{
		Toolsets: []string{"core,ops"},
		Tools:    []string{"diagnose_workflow,trigger_workflow,delete_resource"},
	})
	if err != nil {
		t.Fatalf("tools allowlist failed: %v", err)
	}
	// delete_resource is in admin (not enabled), so only diagnose_workflow + trigger_workflow match.
	if len(filtered) != 2 || filtered[0].Name != "diagnose_workflow" || filtered[1].Name != "trigger_workflow" {
		t.Fatalf("unexpected filtered tools: %+v", filtered)
	}
}

func TestResolveEnabledTools_RejectsUnknowns(t *testing.T) {
	if _, err := ResolveEnabledTools(SelectionConfig{Toolsets: []string{"core,bogus"}}); err == nil {
		t.Fatalf("expected error for unknown toolset")
	}
	if _, err := ResolveEnabledTools(SelectionConfig{Tools: []string{"watch_workflow"}}); err == nil {
		t.Fatalf("expected error for removed/unknown tool watch_workflow")
	}
}
