{{/* Every resource is named <release>-<component>. */}}
{{- define "scout.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- end }}

{{- define "scout.labels" -}}
app.kubernetes.io/name: scout
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{/* Selector labels: pass (dict "root" . "component" "worker"). */}}
{{- define "scout.selector" -}}
app.kubernetes.io/name: scout
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "scout.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{/* In-cluster names other components dial. */}}
{{- define "scout.postgresHost" -}}{{ include "scout.fullname" . }}-postgres{{- end }}
{{- define "scout.coordinatorHost" -}}{{ include "scout.fullname" . }}-coordinator{{- end }}
{{- define "scout.kafkaHost" -}}{{ include "scout.fullname" . }}-kafka{{- end }}
{{- define "scout.fixtureHost" -}}{{ include "scout.fullname" . }}-fixture{{- end }}
{{- define "scout.postgresSecret" -}}
{{- default (printf "%s-postgres" (include "scout.fullname" .)) .Values.postgres.existingSecret -}}
{{- end }}

{{/* Prometheus scrape hints on a pod: pass (dict "port" 9101). Pairs with deploy/prometheus/prometheus-k8s.yml. */}}
{{- define "scout.scrapeAnnotations" -}}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .port | quote }}
prometheus.io/path: /metrics
{{- end }}

{{/* The pod-level hardening every Scout pod shares. The image's user is 10001. */}}
{{- define "scout.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "scout.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}

{{/* SCOUT_DATABASE_URL from the Postgres secret, for the coordinator only (workers hold no credentials). */}}
{{- define "scout.databaseEnv" -}}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "scout.postgresSecret" . }}
      key: password
- name: SCOUT_DATABASE_URL
  value: postgresql://scout:$(POSTGRES_PASSWORD)@{{ include "scout.postgresHost" . }}:5432/scout
{{- end }}

{{/* Block until a TCP port accepts connections: pass (dict "root" . "host" "x" "port" 5432). */}}
{{- define "scout.waitFor" -}}
- name: wait-for-{{ .host }}
  image: {{ include "scout.image" .root }}
  imagePullPolicy: {{ .root.Values.image.pullPolicy }}
  command:
    - bash
    - -c
    - until (exec 3<>/dev/tcp/{{ .host }}/{{ .port }}) 2>/dev/null; do sleep 2; done
  securityContext:
    {{- include "scout.containerSecurityContext" .root | nindent 4 }}
  resources:
    requests: {cpu: 10m, memory: 16Mi}
    limits: {memory: 64Mi}
{{- end }}

{{/* Run a stack.sh role, then park instead of exiting, so a Deployment does not restart a role
     that finished its work: pass the role name. A failure exits non-zero and is restarted. */}}
{{- define "scout.runThenPark" -}}
- bash
- -c
- |
  bash /etc/scout/stack.sh {{ . }} || exit $?
  echo "{{ . }}: finished; parked" >&2
  trap 'exit 0' TERM INT
  while :; do sleep 3600 & wait $!; done
{{- end }}
