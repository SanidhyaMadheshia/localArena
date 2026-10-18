# Platform layer (operators, metrics, GitOps). The application itself is
# deployed by Argo CD from Git (manage_app = false) or directly here.
resource "kubernetes_namespace_v1" "app" {
  metadata {
    name   = var.app_namespace
    labels = { "app.kubernetes.io/part-of" = "localarena" }
  }
}

resource "helm_release" "cnpg" {
  name             = "cnpg"
  namespace        = "cnpg-system"
  create_namespace = true
  repository       = "https://cloudnative-pg.github.io/charts"
  chart            = "cloudnative-pg"
  version          = "0.29.1"
  values           = [file("${path.module}/../kubernetes/helm/cnpg-values.yaml")]
  wait             = true
}

resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  namespace  = "kube-system"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  version    = "3.14.0"
  set        = [{ name = "args[0]", value = "--kubelet-insecure-tls" }] # kind kubelets use self-signed certs
}

resource "helm_release" "argocd" {
  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "10.10.5"
  values           = [file("${path.module}/argocd-values.yaml")]
  wait             = true
  timeout          = 600
}

resource "helm_release" "localarena" {
  count      = var.manage_app ? 1 : 0
  name       = "localarena"
  namespace  = kubernetes_namespace_v1.app.metadata[0].name
  chart      = "${path.module}/../helm/localarena"
  values     = [file("${path.module}/../helm/localarena/values-dev.yaml")]
  set        = [{ name = "image.api.tag", value = var.image_tag }, { name = "image.gameserver.tag", value = var.image_tag }]
  atomic     = true
  timeout    = 600
  depends_on = [helm_release.cnpg]
}
