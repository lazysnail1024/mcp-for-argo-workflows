package curation

import (
	"fmt"
	"sort"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
)

const (
	// GraphFormatMermaid renders a Mermaid flowchart TD diagram.
	GraphFormatMermaid = "mermaid"
	// GraphFormatASCII renders a compact ASCII tree.
	GraphFormatASCII = "ascii"
)

// GraphResult holds a rendered workflow or manifest topology.
type GraphResult struct {
	Graph          string `json:"graph"`
	Format         string `json:"format"`
	NodeCount      int    `json:"nodeCount"`
	CollapsedCount int    `json:"collapsedSucceededCount,omitempty"`
}

// RenderWorkflowGraph renders a live/archived Workflow's execution graph in Mermaid or ASCII,
// optionally collapsing Succeeded leaf branches to keep large DAGs concise.
func RenderWorkflowGraph(wf *wfv1.Workflow, format string, includeStatus, collapseSucceeded bool) (*GraphResult, error) {
	fmtNorm, err := normalizeGraphFormat(format)
	if err != nil {
		return nil, err
	}
	if wf == nil || len(wf.Status.Nodes) == 0 {
		empty := "No nodes in workflow"
		if fmtNorm == GraphFormatMermaid {
			empty = "flowchart TD\n    Empty[\"No nodes in workflow\"]"
		}
		return &GraphResult{Graph: empty, Format: fmtNorm, NodeCount: 0}, nil
	}

	nodeMap := make(map[string]*wfv1.NodeStatus, len(wf.Status.Nodes))
	for id, n := range wf.Status.Nodes {
		copyNode := n
		nodeMap[id] = &copyNode
	}

	collapsed := 0
	visibleIDs := make([]string, 0, len(wf.Status.Nodes))
	visibleSet := make(map[string]bool, len(wf.Status.Nodes))
	for id, n := range nodeMap {
		if !isRenderableNode(n) {
			continue
		}
		if collapseSucceeded && n.Phase == wfv1.NodeSucceeded && (n.Type == wfv1.NodeTypePod || n.Type == wfv1.NodeTypeContainer) && len(n.Children) == 0 {
			collapsed++
			continue
		}
		visibleIDs = append(visibleIDs, id)
		visibleSet[id] = true
	}
	sort.Strings(visibleIDs)

	var rendered string
	if fmtNorm == GraphFormatMermaid {
		rendered = renderRuntimeMermaid(visibleIDs, visibleSet, nodeMap, includeStatus, collapsed)
	} else {
		rendered = renderRuntimeASCII(visibleIDs, visibleSet, nodeMap, includeStatus, collapsed)
	}

	return &GraphResult{
		Graph:          rendered,
		Format:         fmtNorm,
		NodeCount:      len(wf.Status.Nodes),
		CollapsedCount: collapsed,
	}, nil
}

// RenderSpecGraph renders a static WorkflowSpec (from an unsubmitted Workflow,
// WorkflowTemplate, ClusterWorkflowTemplate, or CronWorkflow) as Mermaid or ASCII.
func RenderSpecGraph(spec *wfv1.WorkflowSpec, format string) (*GraphResult, error) {
	fmtNorm, err := normalizeGraphFormat(format)
	if err != nil {
		return nil, err
	}
	if spec == nil || len(spec.Templates) == 0 {
		empty := "No inline templates in spec (workflow may reference a WorkflowTemplate)"
		if fmtNorm == GraphFormatMermaid {
			empty = "flowchart TD\n    Ref[\"No inline templates (check workflowTemplateRef)\"]"
		}
		return &GraphResult{Graph: empty, Format: fmtNorm, NodeCount: 0}, nil
	}

	tmplMap := make(map[string]*wfv1.Template, len(spec.Templates))
	for i := range spec.Templates {
		tmplMap[spec.Templates[i].Name] = &spec.Templates[i]
	}

	type edge struct{ from, to string }
	var edges []edge
	nodes := make(map[string]string)

	addNode := func(id, label string) {
		if _, ok := nodes[id]; !ok {
			nodes[id] = label
		}
	}

	for _, t := range spec.Templates {
		switch {
		case t.DAG != nil:
			addNode(t.Name, t.Name+" (DAG)")
			for _, task := range t.DAG.Tasks {
				taskID := t.Name + "_" + task.Name
				target := task.Template
				if target == "" && task.TemplateRef != nil {
					target = task.TemplateRef.Name + "/" + task.TemplateRef.Template
				}
				label := task.Name
				if target != "" && target != task.Name {
					label = fmt.Sprintf("%s (%s)", task.Name, target)
				}
				addNode(taskID, label)
				if len(task.Dependencies) == 0 && task.Depends == "" {
					edges = append(edges, edge{from: t.Name, to: taskID})
				} else {
					for _, dep := range task.Dependencies {
						edges = append(edges, edge{from: t.Name + "_" + dep, to: taskID})
					}
					if len(task.Dependencies) == 0 && task.Depends != "" {
						edges = append(edges, edge{from: t.Name, to: taskID})
					}
				}
			}
		case len(t.Steps) > 0:
			addNode(t.Name, t.Name+" (Steps)")
			prevGroup := []string{t.Name}
			for gIdx, parallelGroup := range t.Steps {
				currGroup := make([]string, 0, len(parallelGroup.Steps))
				for _, step := range parallelGroup.Steps {
					stepID := fmt.Sprintf("%s_s%d_%s", t.Name, gIdx, step.Name)
					label := step.Name
					if step.Template != "" && step.Template != step.Name {
						label = fmt.Sprintf("%s (%s)", step.Name, step.Template)
					}
					addNode(stepID, label)
					for _, prev := range prevGroup {
						edges = append(edges, edge{from: prev, to: stepID})
					}
					currGroup = append(currGroup, stepID)
				}
				if len(currGroup) > 0 {
					prevGroup = currGroup
				}
			}
		default:
			if spec.Entrypoint == t.Name || len(spec.Templates) == 1 {
				addNode(t.Name, t.Name)
			}
		}
	}

	nodeKeys := make([]string, 0, len(nodes))
	for k := range nodes {
		nodeKeys = append(nodeKeys, k)
	}
	sort.Strings(nodeKeys)

	var sb strings.Builder
	if fmtNorm == GraphFormatMermaid {
		sb.WriteString("flowchart TD\n")
		for _, k := range nodeKeys {
			fmt.Fprintf(&sb, "    %s[%q]\n", makeSafeMermaidID(k), escapeMermaidLabel(nodes[k]))
		}
		seenEdge := map[string]bool{}
		for _, e := range edges {
			key := e.from + "->" + e.to
			if seenEdge[key] {
				continue
			}
			seenEdge[key] = true
			fmt.Fprintf(&sb, "    %s --> %s\n", makeSafeMermaidID(e.from), makeSafeMermaidID(e.to))
		}
	} else {
		sb.WriteString("Entrypoint: " + spec.Entrypoint + "\n")
		children := map[string][]string{}
		for _, e := range edges {
			children[e.from] = append(children[e.from], e.to)
		}
		start := spec.Entrypoint
		if start == "" && len(nodeKeys) > 0 {
			start = nodeKeys[0]
		}
		renderStaticASCII(&sb, start, nodes, children, "", true, map[string]bool{})
	}

	return &GraphResult{
		Graph:     strings.TrimRight(sb.String(), "\n"),
		Format:    fmtNorm,
		NodeCount: len(nodes),
	}, nil
}

func renderStaticASCII(sb *strings.Builder, id string, labels map[string]string, children map[string][]string, prefix string, isLast bool, visited map[string]bool) {
	if visited[id] {
		return
	}
	visited[id] = true
	branch := "├── "
	if isLast {
		branch = "└── "
	}
	label := labels[id]
	if label == "" {
		label = id
	}
	sb.WriteString(prefix + branch + label + "\n")
	kids := children[id]
	for i, kid := range kids {
		nextPrefix := prefix + "│   "
		if isLast {
			nextPrefix = prefix + "    "
		}
		renderStaticASCII(sb, kid, labels, children, nextPrefix, i == len(kids)-1, visited)
	}
}

func renderRuntimeMermaid(
	visibleIDs []string,
	visibleSet map[string]bool,
	nodeMap map[string]*wfv1.NodeStatus,
	includeStatus bool,
	collapsed int,
) string {
	var sb strings.Builder
	sb.WriteString("flowchart TD\n")

	edges := map[string]bool{}
	for _, id := range visibleIDs {
		node := nodeMap[id]
		safeID := makeSafeMermaidID(id)
		label := escapeMermaidLabel(nodeDisplayLabel(node))
		if includeStatus {
			fmt.Fprintf(&sb, "    %s[%q]:::%s\n", safeID, label, phaseClassName(node.Phase))
		} else {
			fmt.Fprintf(&sb, "    %s[%q]\n", safeID, label)
		}
		for _, childID := range node.Children {
			if !visibleSet[childID] {
				continue
			}
			safeChild := makeSafeMermaidID(childID)
			key := safeID + "->" + safeChild
			if !edges[key] {
				edges[key] = true
				fmt.Fprintf(&sb, "    %s --> %s\n", safeID, safeChild)
			}
		}
	}

	if collapsed > 0 {
		fmt.Fprintf(&sb, "    CollapsedSucceeded[\"Succeeded leaf steps collapsed: %d\"]:::succeeded\n", collapsed)
	}

	if includeStatus {
		sb.WriteString("\n")
		sb.WriteString("    classDef succeeded fill:#22c55e,color:#fff,stroke:#16a34a\n")
		sb.WriteString("    classDef failed fill:#ef4444,color:#fff,stroke:#dc2626\n")
		sb.WriteString("    classDef error fill:#dc2626,color:#fff,stroke:#b91c1c\n")
		sb.WriteString("    classDef running fill:#3b82f6,color:#fff,stroke:#2563eb\n")
		sb.WriteString("    classDef pending fill:#9ca3af,color:#fff,stroke:#6b7280\n")
		sb.WriteString("    classDef skipped fill:#d1d5db,color:#374151,stroke:#9ca3af\n")
		sb.WriteString("    classDef omitted fill:#e5e7eb,color:#6b7280,stroke:#d1d5db\n")
	}

	return strings.TrimRight(sb.String(), "\n")
}

func renderRuntimeASCII(
	visibleIDs []string,
	visibleSet map[string]bool,
	nodeMap map[string]*wfv1.NodeStatus,
	includeStatus bool,
	collapsed int,
) string {
	hasParent := map[string]bool{}
	for _, id := range visibleIDs {
		for _, childID := range nodeMap[id].Children {
			if visibleSet[childID] {
				hasParent[childID] = true
			}
		}
	}
	var roots []string
	for _, id := range visibleIDs {
		if !hasParent[id] {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)

	var sb strings.Builder
	visited := map[string]bool{}
	for i, rootID := range roots {
		renderASCIINode(&sb, rootID, visibleSet, nodeMap, "", i == len(roots)-1, includeStatus, visited)
	}
	if collapsed > 0 {
		fmt.Fprintf(&sb, "... (%d succeeded leaf node(s) collapsed)\n", collapsed)
	}
	return strings.TrimRight(sb.String(), "\n")
}

func renderASCIINode(
	sb *strings.Builder,
	id string,
	visibleSet map[string]bool,
	nodeMap map[string]*wfv1.NodeStatus,
	prefix string,
	isLast bool,
	includeStatus bool,
	visited map[string]bool,
) {
	if visited[id] {
		return
	}
	visited[id] = true
	node := nodeMap[id]
	branch := "├── "
	if isLast {
		branch = "└── "
	}
	line := prefix + branch + nodeDisplayLabel(node)
	if includeStatus {
		line += " " + phaseSymbol(node.Phase)
	}
	sb.WriteString(line + "\n")

	var children []string
	for _, c := range node.Children {
		if visibleSet[c] {
			children = append(children, c)
		}
	}
	for i, c := range children {
		nextPrefix := prefix + "│   "
		if isLast {
			nextPrefix = prefix + "    "
		}
		renderASCIINode(sb, c, visibleSet, nodeMap, nextPrefix, i == len(children)-1, includeStatus, visited)
	}
}

func normalizeGraphFormat(format string) (string, error) {
	f := strings.ToLower(strings.TrimSpace(format))
	if f == "" {
		return GraphFormatMermaid, nil
	}
	if f != GraphFormatMermaid && f != GraphFormatASCII {
		return "", fmt.Errorf("unsupported graph format %q: must be %q or %q", format, GraphFormatMermaid, GraphFormatASCII)
	}
	return f, nil
}

func isRenderableNode(node *wfv1.NodeStatus) bool {
	if node == nil {
		return false
	}
	switch node.Type {
	case wfv1.NodeTypeRetry, wfv1.NodeTypeStepGroup, wfv1.NodeTypeTaskGroup:
		return false
	default:
		return true
	}
}

func nodeDisplayLabel(node *wfv1.NodeStatus) string {
	if node.DisplayName != "" {
		return node.DisplayName
	}
	if node.TemplateName != "" {
		return node.TemplateName
	}
	return node.Name
}

func makeSafeMermaidID(id string) string {
	var sb strings.Builder
	sb.Grow(len(id) + 2)
	sb.WriteString("n_")
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteByte('_')
		}
	}
	return sb.String()
}

func escapeMermaidLabel(s string) string {
	s = strings.ReplaceAll(s, `"`, `'`)
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

func phaseClassName(phase wfv1.NodePhase) string {
	switch phase {
	case wfv1.NodeSucceeded:
		return "succeeded"
	case wfv1.NodeFailed:
		return "failed"
	case wfv1.NodeError:
		return "error"
	case wfv1.NodeRunning:
		return "running"
	case wfv1.NodeSkipped:
		return "skipped"
	case wfv1.NodeOmitted:
		return "omitted"
	default:
		return "pending"
	}
}

func phaseSymbol(phase wfv1.NodePhase) string {
	switch phase {
	case wfv1.NodeSucceeded:
		return "[Succeeded]"
	case wfv1.NodeFailed:
		return "[Failed]"
	case wfv1.NodeError:
		return "[Error]"
	case wfv1.NodeRunning:
		return "[Running]"
	case wfv1.NodeSkipped:
		return "[Skipped]"
	case wfv1.NodeOmitted:
		return "[Omitted]"
	default:
		return "[Pending]"
	}
}
