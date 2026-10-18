terraform {
  required_version = ">= 1.13.0"
  required_providers {
    helm       = { source = "hashicorp/helm", version = "3.3.0" }
    kubernetes = { source = "hashicorp/kubernetes", version = "3.3.0" }
  }
  # Local state for the lab (adr/0020): infra/opentofu/terraform.tfstate,
  # gitignored. It holds no secrets (none are passed through Tofu); back it up
  # with the repo checkout. A shared setup would use a remote backend + locking.
}

provider "kubernetes" {
  config_path    = var.kubeconfig
  config_context = var.kube_context
}

provider "helm" {
  kubernetes = {
    config_path    = var.kubeconfig
    config_context = var.kube_context
  }
}
