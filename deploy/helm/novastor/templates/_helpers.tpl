{{/*
Expand the name of the chart.
*/}}
{{- define "novastor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "novastor.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "novastor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "novastor.labels" -}}
helm.sh/chart: {{ include "novastor.chart" . }}
{{ include "novastor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "novastor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "novastor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "novastor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "novastor.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Global image registry helper
*/}}
{{- define "novastor.imageRegistry" -}}
{{- .Values.global.image.registry | default "ghcr.io/piwi3910/novastor" }}
{{- end }}

{{/*
Global image tag helper
*/}}
{{- define "novastor.imageTag" -}}
{{- .Values.global.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
TLS enabled helper - returns "true" if TLS should be enabled, "" otherwise
This helper properly handles the case where global.tls.enabled is explicitly set to false
*/}}
{{- define "novastor.tls.enabled" -}}
{{- if hasKey .Values.global "tls" }}
  {{- if hasKey .Values.global.tls "enabled" }}
    {{- if .Values.global.tls.enabled }}true{{- end }}
  {{- else }}
    {{- true }}
  {{- end }}
{{- else }}
  {{- true }}
{{- end }}
{{- end }}

{{/*
TLS certificate volume name
*/}}
{{- define "novastor.tls.volumeName" -}}
{{- printf "%s-tls-certs" (include "novastor.fullname" .) }}
{{- end }}

{{/*
TLS certificate mount path
*/}}
{{- define "novastor.tls.mountPath" -}}
/var/run/novastor/tls
{{- end }}

{{/*
TLS CA certificate path
*/}}
{{- define "novastor.tls.caPath" -}}
{{- printf "%s/ca.crt" (include "novastor.tls.mountPath" .) }}
{{- end }}

{{/*
TLS server certificate path (for components that accept connections)
*/}}
{{- define "novastor.tls.serverCertPath" -}}
{{- printf "%s/server.crt" (include "novastor.tls.mountPath" .) }}
{{- end }}

{{/*
TLS server key path (for components that accept connections)
*/}}
{{- define "novastor.tls.serverKeyPath" -}}
{{- printf "%s/server.key" (include "novastor.tls.mountPath" .) }}
{{- end }}

{{/*
TLS client certificate path (for components that initiate connections)
*/}}
{{- define "novastor.tls.clientCertPath" -}}
{{- printf "%s/client.crt" (include "novastor.tls.mountPath" .) }}
{{- end }}

{{/*
TLS client key path (for components that initiate connections)
*/}}
{{- define "novastor.tls.clientKeyPath" -}}
{{- printf "%s/client.key" (include "novastor.tls.mountPath" .) }}
{{- end }}
