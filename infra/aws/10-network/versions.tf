terraform {
  required_version = ">= 1.13.0"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 6.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
  # Local state (gitignored). 20-eks reads it via terraform_remote_state.
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "localarena", stack = "10-network", managed-by = "opentofu" }
  }
}
