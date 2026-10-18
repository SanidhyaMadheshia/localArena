variable "kubeconfig" {
  type    = string
  default = "~/.kube/config"
}
variable "kube_context" {
  type    = string
  default = "kind-localarena"
}
variable "app_namespace" {
  type    = string
  default = "localarena"
}
variable "image_tag" {
  description = "Image tag for api/gameserver (CI sets the commit SHA)."
  type        = string
  default     = "dev"
}
variable "manage_app" {
  description = "Install the app chart with Tofu. Set false when Argo CD owns the app (GitOps)."
  type        = bool
  default     = false
}
