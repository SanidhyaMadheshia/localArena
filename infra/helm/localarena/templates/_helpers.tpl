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
{{- define "la.zoneSpread" -}}
- maxSkew: 1
  topologyKey: topology.kubernetes.io/zone
  whenUnsatisfiable: DoNotSchedule
  nodeTaintsPolicy: Honor
  labelSelector: { matchLabels: { app: {{ .app }} } }
  matchLabelKeys: [pod-template-hash]
{{- end }}
{{- define "la.spread" -}}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    nodeTaintsPolicy: Honor
    labelSelector: { matchLabels: { app: {{ .app }} } }
    matchLabelKeys: [pod-template-hash]
  {{- if .root.Values.spread.zone }}
  {{- include "la.zoneSpread" . | nindent 2 }}
  {{- end }}
{{- end }}
{{- /* image reference: optional global registry prefix (ECR) */ -}}
{{- define "la.image" -}}
{{ with .root.Values.image.registry }}{{ . }}/{{ end }}{{ .img.repository }}:{{ .tag }}
{{- end }}
{{- /* Kafka client env for one service; .user is the KafkaUser (and Secret) name. */ -}}
{{- define "la.kafkaEnv" -}}
{{- $k := .root.Values.kafka }}
{{- if $k.bootstrap }}
- { name: KAFKA_BROKERS, value: {{ $k.bootstrap | quote }} }
{{- if $k.auth.enabled }}
- { name: KAFKA_SECURITY_PROTOCOL, value: SASL_SSL }
- { name: KAFKA_SASL_MECHANISM, value: {{ $k.auth.mechanism | quote }} }
- { name: KAFKA_USERNAME, value: {{ .user | quote }} }
- name: KAFKA_PASSWORD
  valueFrom: { secretKeyRef: { name: {{ .user | quote }}, key: password } }
- { name: KAFKA_CA_FILE, value: /etc/kafka-ca/{{ $k.auth.caKey }} }
{{- end }}
{{- end }}
{{- end }}
{{- define "la.kafkaCAMount" -}}
{{- if and .Values.kafka.bootstrap .Values.kafka.auth.enabled }}
- { name: kafka-ca, mountPath: /etc/kafka-ca, readOnly: true }
{{- end }}
{{- end }}
{{- define "la.kafkaCAVolume" -}}
{{- if and .Values.kafka.bootstrap .Values.kafka.auth.enabled }}
- name: kafka-ca
  secret: { secretName: {{ .Values.kafka.auth.caSecret | quote }}, items: [{ key: {{ .Values.kafka.auth.caKey | quote }}, path: {{ .Values.kafka.auth.caKey | quote }} }] }
{{- end }}
{{- end }}
{{- define "la.dbEnv" -}}
- name: DB_PASSWORD
  valueFrom: { secretKeyRef: { name: arena-db-app, key: password } }
{{- end }}
