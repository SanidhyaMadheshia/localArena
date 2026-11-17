terraform {
  required_version = ">= 1.13.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  # Local state (gitignored).
}

provider "aws" {
  region = local.region
  default_tags {
    tags = { project = "localarena", stack = "20-eks", managed-by = "opentofu" }
  }
}
