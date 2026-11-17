variable "admin_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint. Required, no default. scripts/aws-up.sh passes your current IP as x.x.x.x/32."
  type        = list(string)
  validation {
    condition     = length(var.admin_cidrs) > 0 && !contains(var.admin_cidrs, "0.0.0.0/0")
    error_message = "admin_cidrs must be non-empty and must not contain 0.0.0.0/0."
  }
}

variable "admin_principal_arn" {
  description = "IAM user or role ARN that gets cluster-admin through an EKS access entry (not an assumed-role session ARN, not the account root)."
  type        = string
}

variable "kubernetes_version" {
  # EKS newest version is 1.37 (AWS docs "Review release notes for Kubernetes versions on standard
  # support", checked 2026-11-17). Default is newest minus one so drill 014 can upgrade 1.36 -> 1.37.
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.36"
}

variable "instance_type" {
  description = "Node instance type for the general group. Fallbacks if not offered: m6i.large, m5.large."
  type        = string
  default     = "m7i.large"
}

variable "kafka_instance_type" {
  description = "Node instance type for the kafka group. null = same as instance_type."
  type        = string
  default     = null
}

variable "capacity_type" {
  description = "ON_DEMAND or SPOT."
  type        = string
  default     = "ON_DEMAND"
}

variable "general_nodes_per_az" {
  description = "Desired nodes per AZ in the general groups. Set 0 to scale to zero (3 groups, one per AZ)."
  type        = number
  default     = 1
}

variable "kafka_nodes_per_az" {
  description = "Desired nodes per AZ in the kafka groups. Set 0 to scale to zero."
  type        = number
  default     = 1
}

variable "node_max_per_az" {
  description = "Max nodes per AZ group (headroom for rolling updates and the add-broker drill)."
  type        = number
  default     = 2
}

variable "node_disk_gb" {
  type    = number
  default = 50
}

variable "cluster_log_types" {
  description = "Control plane log types sent to CloudWatch."
  type        = list(string)
  default     = ["audit"]
}

variable "cnpg_namespace" {
  type    = string
  default = "localarena"
}

variable "cnpg_service_account" {
  description = "CNPG creates a service account named after the Cluster (infra/helm/localarena/templates/postgres.yaml: arena-db)."
  type        = string
  default     = "arena-db"
}

variable "cnpg_extra_service_accounts" {
  description = "Extra service accounts in cnpg_namespace that get the backup role (restore drill cluster)."
  type        = list(string)
  default     = ["arena-db-restore"]
}
