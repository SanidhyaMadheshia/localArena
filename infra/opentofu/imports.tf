# Adopt resources created before OpenTofu managed them (weeks 2-7), instead of
# destroying and recreating them. Safe to keep: import is a no-op once in state.
import {
  to = kubernetes_namespace_v1.app
  id = "localarena"
}
import {
  to = helm_release.cnpg
  id = "cnpg-system/cnpg"
}
import {
  to = helm_release.metrics_server
  id = "kube-system/metrics-server"
}
