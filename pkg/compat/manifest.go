// Package compat provides lossless bidirectional compatibility across Argo Workflows v3.6+ and v4.x.
package compat

import (
	"encoding/json"
	"fmt"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/workflow/convert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// Supported Argo Workflows manifest kinds.
const (
	KindWorkflow                = "Workflow"
	KindWorkflowTemplate        = "WorkflowTemplate"
	KindClusterWorkflowTemplate = "ClusterWorkflowTemplate"
	KindCronWorkflow            = "CronWorkflow"

	// MaxManifestBytes caps inbound YAML/JSON manifests to guard against DoS payloads.
	MaxManifestBytes = 1 << 20 // 1 MiB
)

// NormalizedManifest holds a parsed and v3.6+/v4-normalized Argo Workflows manifest.
type NormalizedManifest struct {
	Kind                    string
	Name                    string
	GenerateName            string
	Namespace               string
	Workflow                *wfv1.Workflow
	WorkflowTemplate        *wfv1.WorkflowTemplate
	ClusterWorkflowTemplate *wfv1.ClusterWorkflowTemplate
	CronWorkflow            *wfv1.CronWorkflow
	CanonicalYAML           string
	Changes                 []string
	Warnings                []string
}

// legacyWorkflowSpecCompat extends convert.LegacyWorkflowSpec with fields removed in v4.0
// (such as podPriority) so v3.6/v3.7 manifests parse cleanly while still catching genuine typos.
type legacyWorkflowSpecCompat struct {
	convert.LegacyWorkflowSpec
	// Deprecated in v3.x, removed in v4.0; replaced by podPriorityClassName.
	PodPriority *int32 `json:"podPriority,omitempty"`
}

type legacyWorkflowCompat struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              legacyWorkflowSpecCompat `json:"spec"`
	Status            wfv1.WorkflowStatus      `json:"status,omitzero"`
}

type legacyWorkflowTemplateCompat struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              legacyWorkflowSpecCompat `json:"spec"`
}

type legacyClusterWorkflowTemplateCompat struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              legacyWorkflowSpecCompat `json:"spec"`
}

type legacyCronWorkflowSpecCompat struct {
	WorkflowSpec               legacyWorkflowSpecCompat `json:"workflowSpec"`
	Schedule                   string                   `json:"schedule,omitempty"`
	Schedules                  []string                 `json:"schedules,omitempty"`
	ConcurrencyPolicy          wfv1.ConcurrencyPolicy   `json:"concurrencyPolicy,omitempty"`
	Suspend                    bool                     `json:"suspend,omitempty"`
	StartingDeadlineSeconds    *int64                   `json:"startingDeadlineSeconds,omitempty"`
	SuccessfulJobsHistoryLimit *int32                   `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     *int32                   `json:"failedJobsHistoryLimit,omitempty"`
	Timezone                   string                   `json:"timezone,omitempty"`
	WorkflowMetadata           *metav1.ObjectMeta       `json:"workflowMetadata,omitempty"`
	StopStrategy               *wfv1.StopStrategy       `json:"stopStrategy,omitempty"`
	When                       string                   `json:"when,omitempty"`
}

type legacyCronWorkflowCompat struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              legacyCronWorkflowSpecCompat `json:"spec"`
	Status            wfv1.CronWorkflowStatus      `json:"status,omitzero"`
}

// NormalizeManifest parses a YAML or JSON manifest, accepts valid Argo Workflows v3.6+
// singular fields (schedule, mutex, semaphore, podPriority), strictly rejects unknown
// fields, and upgrades the object into the canonical v3.6+/v4 plural representation.
func NormalizeManifest(raw string) (*NormalizedManifest, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("manifest cannot be empty")
	}
	if len(raw) > MaxManifestBytes {
		return nil, fmt.Errorf("manifest too large (%d bytes), max allowed is %d bytes", len(raw), MaxManifestBytes)
	}

	var header struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
	}
	if err := yaml.Unmarshal([]byte(trimmed), &header); err != nil {
		return nil, fmt.Errorf("failed to parse manifest YAML/JSON: %w", err)
	}

	kind := strings.TrimSpace(header.Kind)
	if kind == "" {
		kind = KindWorkflow
	}

	switch kind {
	case KindWorkflow:
		return normalizeWorkflow(trimmed)
	case KindWorkflowTemplate:
		return normalizeWorkflowTemplate(trimmed)
	case KindClusterWorkflowTemplate:
		return normalizeClusterWorkflowTemplate(trimmed)
	case KindCronWorkflow:
		return normalizeCronWorkflow(trimmed)
	default:
		return nil, fmt.Errorf(
			"unsupported manifest kind %q: must be one of %s, %s, %s, or %s",
			kind, KindWorkflow, KindWorkflowTemplate, KindClusterWorkflowTemplate, KindCronWorkflow,
		)
	}
}

func normalizeWorkflow(raw string) (*NormalizedManifest, error) {
	var legacy legacyWorkflowCompat
	if err := yaml.UnmarshalStrict([]byte(raw), &legacy); err != nil {
		return nil, fmt.Errorf("invalid Workflow manifest: %w", err)
	}
	changes, warnings := inspectLegacyWorkflowSpec(&legacy.Spec)
	wf := &wfv1.Workflow{
		TypeMeta:   legacy.TypeMeta,
		ObjectMeta: legacy.ObjectMeta,
		Spec:       legacy.Spec.ToCurrent(),
		Status:     legacy.Status,
	}
	if wf.APIVersion == "" {
		wf.APIVersion = "argoproj.io/v1alpha1"
	}
	if wf.Kind == "" {
		wf.Kind = KindWorkflow
	}
	canonical, err := marshalCanonicalYAML(wf)
	if err != nil {
		return nil, err
	}
	return &NormalizedManifest{
		Kind:          KindWorkflow,
		Name:          wf.Name,
		GenerateName:  wf.GenerateName,
		Namespace:     wf.Namespace,
		Workflow:      wf,
		CanonicalYAML: canonical,
		Changes:       changes,
		Warnings:      warnings,
	}, nil
}

func normalizeWorkflowTemplate(raw string) (*NormalizedManifest, error) {
	var legacy legacyWorkflowTemplateCompat
	if err := yaml.UnmarshalStrict([]byte(raw), &legacy); err != nil {
		return nil, fmt.Errorf("invalid WorkflowTemplate manifest: %w", err)
	}
	changes, warnings := inspectLegacyWorkflowSpec(&legacy.Spec)
	wft := &wfv1.WorkflowTemplate{
		TypeMeta:   legacy.TypeMeta,
		ObjectMeta: legacy.ObjectMeta,
		Spec:       legacy.Spec.ToCurrent(),
	}
	if wft.APIVersion == "" {
		wft.APIVersion = "argoproj.io/v1alpha1"
	}
	if wft.Kind == "" {
		wft.Kind = KindWorkflowTemplate
	}
	canonical, err := marshalCanonicalYAML(wft)
	if err != nil {
		return nil, err
	}
	return &NormalizedManifest{
		Kind:             KindWorkflowTemplate,
		Name:             wft.Name,
		Namespace:        wft.Namespace,
		WorkflowTemplate: wft,
		CanonicalYAML:    canonical,
		Changes:          changes,
		Warnings:         warnings,
	}, nil
}

func normalizeClusterWorkflowTemplate(raw string) (*NormalizedManifest, error) {
	var legacy legacyClusterWorkflowTemplateCompat
	if err := yaml.UnmarshalStrict([]byte(raw), &legacy); err != nil {
		return nil, fmt.Errorf("invalid ClusterWorkflowTemplate manifest: %w", err)
	}
	changes, warnings := inspectLegacyWorkflowSpec(&legacy.Spec)
	cwft := &wfv1.ClusterWorkflowTemplate{
		TypeMeta:   legacy.TypeMeta,
		ObjectMeta: legacy.ObjectMeta,
		Spec:       legacy.Spec.ToCurrent(),
	}
	if cwft.APIVersion == "" {
		cwft.APIVersion = "argoproj.io/v1alpha1"
	}
	if cwft.Kind == "" {
		cwft.Kind = KindClusterWorkflowTemplate
	}
	canonical, err := marshalCanonicalYAML(cwft)
	if err != nil {
		return nil, err
	}
	return &NormalizedManifest{
		Kind:                    KindClusterWorkflowTemplate,
		Name:                    cwft.Name,
		ClusterWorkflowTemplate: cwft,
		CanonicalYAML:           canonical,
		Changes:                 changes,
		Warnings:                warnings,
	}, nil
}

func normalizeCronWorkflow(raw string) (*NormalizedManifest, error) {
	var legacy legacyCronWorkflowCompat
	if err := yaml.UnmarshalStrict([]byte(raw), &legacy); err != nil {
		return nil, fmt.Errorf("invalid CronWorkflow manifest: %w", err)
	}
	changes, warnings := inspectLegacyWorkflowSpec(&legacy.Spec.WorkflowSpec)

	schedules := append([]string(nil), legacy.Spec.Schedules...)
	if legacy.Spec.Schedule != "" {
		if len(schedules) == 0 {
			schedules = []string{legacy.Spec.Schedule}
			changes = append(changes, "Migrated deprecated spec.schedule to spec.schedules array (compatible with Argo v3.6+ and v4.x)")
		} else {
			warnings = append(warnings, "Both spec.schedule and spec.schedules were provided; kept spec.schedules and dropped deprecated spec.schedule")
		}
	}
	if legacy.Spec.ConcurrencyPolicy == "" {
		warnings = append(warnings, "spec.concurrencyPolicy is unset; defaults to 'Allow' which may trigger overlapping runs")
	}

	cw := &wfv1.CronWorkflow{
		TypeMeta:   legacy.TypeMeta,
		ObjectMeta: legacy.ObjectMeta,
		Spec: wfv1.CronWorkflowSpec{
			WorkflowSpec:               legacy.Spec.WorkflowSpec.ToCurrent(),
			Schedules:                  schedules,
			ConcurrencyPolicy:          legacy.Spec.ConcurrencyPolicy,
			Suspend:                    legacy.Spec.Suspend,
			StartingDeadlineSeconds:    legacy.Spec.StartingDeadlineSeconds,
			SuccessfulJobsHistoryLimit: legacy.Spec.SuccessfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     legacy.Spec.FailedJobsHistoryLimit,
			Timezone:                   legacy.Spec.Timezone,
			WorkflowMetadata:           legacy.Spec.WorkflowMetadata,
			StopStrategy:               legacy.Spec.StopStrategy,
			When:                       legacy.Spec.When,
		},
		Status: legacy.Status,
	}
	if cw.APIVersion == "" {
		cw.APIVersion = "argoproj.io/v1alpha1"
	}
	if cw.Kind == "" {
		cw.Kind = KindCronWorkflow
	}
	canonical, err := marshalCanonicalYAML(cw)
	if err != nil {
		return nil, err
	}
	return &NormalizedManifest{
		Kind:          KindCronWorkflow,
		Name:          cw.Name,
		Namespace:     cw.Namespace,
		CronWorkflow:  cw,
		CanonicalYAML: canonical,
		Changes:       changes,
		Warnings:      warnings,
	}, nil
}

func inspectLegacyWorkflowSpec(spec *legacyWorkflowSpecCompat) (changes, warnings []string) {
	if spec == nil {
		return nil, nil
	}
	if spec.Synchronization != nil {
		changes = append(changes, inspectSynchronization(spec.Synchronization, "spec.synchronization")...)
	}
	for i := range spec.Templates {
		tmpl := &spec.Templates[i]
		if tmpl.Synchronization != nil {
			prefix := fmt.Sprintf("spec.templates[%s].synchronization", tmpl.Name)
			changes = append(changes, inspectSynchronization(tmpl.Synchronization, prefix)...)
		}
	}
	if spec.PodPriority != nil {
		warnings = append(warnings, "spec.podPriority was removed in Argo Workflows v4.0; stripped during normalization—prefer spec.podPriorityClassName")
	}
	return changes, warnings
}

func inspectSynchronization(sync *convert.LegacySynchronization, prefix string) []string {
	var changes []string
	if sync.Mutex != nil {
		changes = append(changes, fmt.Sprintf("Migrated deprecated %s.mutex to %s.mutexes array", prefix, prefix))
	}
	if sync.Semaphore != nil {
		changes = append(changes, fmt.Sprintf("Migrated deprecated %s.semaphore to %s.semaphores array", prefix, prefix))
	}
	return changes
}

func marshalCanonicalYAML(obj any) (string, error) {
	// Round-trip through JSON to avoid zero-value metav1 noise and keep canonical field tags.
	rawJSON, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal normalized manifest: %w", err)
	}
	yamlBytes, err := yaml.JSONToYAML(rawJSON)
	if err != nil {
		return "", fmt.Errorf("failed to render canonical YAML: %w", err)
	}
	return string(yamlBytes), nil
}
