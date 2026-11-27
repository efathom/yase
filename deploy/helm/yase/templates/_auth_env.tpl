{{/*
Auth, TLS-less internal token and rate-limit environment shared by the
gateway and indexer.
*/}}
{{- define "yase.authEnv" -}}
- name: YASE_AUTH_ENABLED
  value: "{{ .Values.auth.enabled }}"
- name: YASE_AUTH_METHOD
  value: "{{ .Values.auth.method }}"
- name: YASE_AUTH_JWT_ISSUER
  value: "{{ .Values.auth.jwt.issuer }}"
- name: YASE_AUTH_JWT_AUDIENCE
  value: "{{ .Values.auth.jwt.audience }}"
- name: YASE_AUTH_JWT_JWKS_URL
  value: "{{ .Values.auth.jwt.jwksUrl }}"
- name: YASE_AUTH_JWT_REQUIRE_TENANT
  value: "{{ .Values.auth.jwt.requireTenant }}"
- name: YASE_AUTH_JWT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Release.Name }}-secrets
      key: jwt-secret
- name: YASE_AUTH_CLIENT_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Release.Name }}-secrets
      key: internal-token
{{- end -}}
