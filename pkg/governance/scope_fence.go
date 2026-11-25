package governance

import (
	"fmt"
	"strings"
)

// DefaultDeniedNamespaces protects Kubernetes system control-plane namespaces by default.
var DefaultDeniedNamespaces = []string{"kube-system"}

// ScopeFence enforces bidirectional namespace allow/deny policies and high-risk
// resource-class locks (ClusterWorkflowTemplate writes and Archive deletion).
type ScopeFence struct {
	AllowedNamespaces     map[string]struct{}
	DeniedNamespaces      map[string]struct{}
	AllowClusterTemplates bool
	AllowArchiveDelete    bool
}

// ScopeFenceConfig configures a ScopeFence instance.
type ScopeFenceConfig struct {
	AllowedNamespaces     []string
	DeniedNamespaces      []string
	AllowClusterTemplates bool
	AllowArchiveDelete    bool
}

// NewScopeFence constructs a normalized ScopeFence.
func NewScopeFence(cfg ScopeFenceConfig) *ScopeFence {
	return &ScopeFence{
		AllowedNamespaces:     toNormalizedSet(cfg.AllowedNamespaces),
		DeniedNamespaces:      toNormalizedSet(cfg.DeniedNamespaces),
		AllowClusterTemplates: cfg.AllowClusterTemplates,
		AllowArchiveDelete:    cfg.AllowArchiveDelete,
	}
}

// IsNamespaceAllowed reports whether namespace passes both the deny list and optional allow list.
func (f *ScopeFence) IsNamespaceAllowed(namespace string) bool {
	if f == nil {
		return true
	}
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		// All-namespace listing is permitted only when no strict allowlist restricts visibility;
		// callers still filter returned items individually via IsNamespaceAllowed.
		return true
	}
	if _, denied := f.DeniedNamespaces[ns]; denied {
		return false
	}
	if len(f.AllowedNamespaces) > 0 {
		_, allowed := f.AllowedNamespaces[ns]
		return allowed
	}
	return true
}

// CheckNamespace enforces access control on a concrete namespace before invoking downstream APIs.
func (f *ScopeFence) CheckNamespace(namespace string) error {
	if f == nil {
		return nil
	}
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		return nil
	}
	if _, denied := f.DeniedNamespaces[ns]; denied {
		return fmt.Errorf("access to namespace %q is blocked by MCP_DENIED_NAMESPACES policy", ns)
	}
	if len(f.AllowedNamespaces) > 0 {
		if _, allowed := f.AllowedNamespaces[ns]; !allowed {
			return fmt.Errorf("access to namespace %q is outside MCP_ALLOWED_NAMESPACES policy", ns)
		}
	}
	return nil
}

// CheckClusterTemplateMutation enforces MCP_ALLOW_CLUSTER_TEMPLATES for ClusterWorkflowTemplate writes.
func (f *ScopeFence) CheckClusterTemplateMutation() error {
	if f != nil && !f.AllowClusterTemplates {
		return fmt.Errorf("modifying or deleting ClusterWorkflowTemplate is disabled by default; set MCP_ALLOW_CLUSTER_TEMPLATES=true to enable")
	}
	return nil
}

// CheckArchiveDelete enforces MCP_ALLOW_ARCHIVE_DELETE for deleting archived workflow history.
func (f *ScopeFence) CheckArchiveDelete() error {
	if f != nil && !f.AllowArchiveDelete {
		return fmt.Errorf("deleting archived workflows is disabled by default to preserve audit history; set MCP_ALLOW_ARCHIVE_DELETE=true to enable")
	}
	return nil
}

func toNormalizedSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				set[trimmed] = struct{}{}
			}
		}
	}
	if len(set) == 0 {
		return nil
	}
	return set
}
