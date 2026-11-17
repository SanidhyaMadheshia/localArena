terraform {
  required_version = ">= 1.13.0"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
  # Local state (gitignored). Optional stack for experiment 015.
}

provider "aws" {
  region = local.region
  default_tags {
    tags = { project = "localarena", stack = "30-msk", managed-by = "opentofu" }
  }
}
