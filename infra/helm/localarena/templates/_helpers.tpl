{{- define "la.labels" -}}
app.kubernetes.io/part-of: localarena
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
{{- define "la.podSecurity" -}}
securityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault
{{- end }}
{{- define "la.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: [ALL]
{{- end }}
{{- define "la.spread" -}}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    nodeTaintsPolicy: Honor
    labelSelector: { matchLabels: { app: {{ . }} } }
    matchLabelKeys: [pod-template-hash]
{{- end }}
{{- define "la.dbEnv" -}}
- name: DB_PASSWORD
  valueFrom: { secretKeyRef: { name: arena-db-app, key: password } }
{{- end }}
