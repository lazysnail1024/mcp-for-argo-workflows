{{- define "mcp-argo-workflows.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mcp-argo-workflows.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "mcp-argo-workflows.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mcp-argo-workflows.labels" -}}
helm.sh/chart: {{ include "mcp-argo-workflows.chart" . }}
{{ include "mcp-argo-workflows.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "mcp-argo-workflows.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mcp-argo-workflows.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "mcp-argo-workflows.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mcp-argo-workflows.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "mcp-argo-workflows.secretName" -}}
{{- if .Values.secrets.create }}
{{- printf "%s-secrets" (include "mcp-argo-workflows.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- required "secrets.existingSecret is required when secrets.create is false" .Values.secrets.existingSecret }}
{{- end }}
{{- end }}

{{- define "mcp-argo-workflows.registrySecretName" -}}
{{- if .Values.argo.tokenRegistry.createSecret }}
{{- printf "%s-token-registry" (include "mcp-argo-workflows.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- required "argo.tokenRegistry.existingSecret is required when argo.tokenRegistry.enabled=true and createSecret=false" .Values.argo.tokenRegistry.existingSecret }}
{{- end }}
{{- end }}

{{- define "mcp-argo-workflows.caConfigMapName" -}}
{{- printf "%s-argo-ca" (include "mcp-argo-workflows.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mcp-argo-workflows.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag }}
{{- end }}
