package argo

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// FormatAgentError rewrites raw gRPC / Kubernetes / HTTP errors into actionable,
// Agent-friendly self-healing guidance referencing registered MCP tools.
func FormatAgentError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrArchivedWorkflowsNotSupported) {
		return fmt.Errorf("%s failed: %w. Switch to an Argo Server deployment if workflow archive access is required", operation, err)
	}

	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return fmt.Errorf("%s: resource not found (%s). Verify the exact name, uid, and namespace using list_workflows, list_templates, or inspect_cron_workflows", operation, st.Message())
		case codes.Unauthenticated:
			return fmt.Errorf("%s: authentication failed against Argo Server (%s). Verify the configured token or check list_clusters for registered instances", operation, st.Message())
		case codes.PermissionDenied:
			return fmt.Errorf("%s: permission denied by RBAC (%s). Narrow your query to an authorized namespace or check cluster permissions", operation, st.Message())
		case codes.InvalidArgument:
			return fmt.Errorf("%s: invalid request argument (%s). Run validate_manifest first to check schema compatibility", operation, st.Message())
		case codes.AlreadyExists:
			return fmt.Errorf("%s: resource already exists (%s)", operation, st.Message())
		case codes.DeadlineExceeded:
			return fmt.Errorf("%s: downstream request timed out (%s). Narrow the query with namespace, labels, or smaller limit/tailLines", operation, st.Message())
		default:
			return fmt.Errorf("%s failed [%s]: %s", operation, st.Code().String(), st.Message())
		}
	}

	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%s: resource not found (%v). Verify the exact name and namespace using list_workflows, list_templates, or inspect_cron_workflows", operation, err)
	}
	if apierrors.IsUnauthorized(err) || apierrors.IsForbidden(err) {
		return fmt.Errorf("%s: Kubernetes RBAC denied access (%v). Verify namespace permissions or target context via list_clusters", operation, err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "connection refused") {
		return fmt.Errorf("%s: unable to reach Argo backend (%v). Verify endpoint connectivity via list_clusters", operation, err)
	}
	return fmt.Errorf("%s failed: %w", operation, err)
}

// IsNotFoundError reports whether err indicates a missing workflow or Kubernetes object.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsNotFound(err) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "not found") || strings.Contains(lower, "404")
}
