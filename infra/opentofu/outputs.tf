output "releases" {
  value = {
    cnpg           = helm_release.cnpg.metadata.version
    metrics_server = helm_release.metrics_server.metadata.version
    argocd         = helm_release.argocd.metadata.version
  }
}
