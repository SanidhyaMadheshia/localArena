variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix. Also the EKS cluster name used for subnet discovery tags (must equal 20-eks cluster_name)."
  type        = string
  default     = "localarena"
}

variable "cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "azs" {
  description = "AZ names. Empty = first 3 available AZs of the region."
  type        = list(string)
  default     = []
}

variable "nat_gateway_count" {
  description = "0 = no NAT (private nodes lose internet), 1 = single NAT (cheap, one AZ is a SPOF for egress), 3 = one per AZ."
  type        = number
  default     = 1
  validation {
    condition     = contains([0, 1, 3], var.nat_gateway_count)
    error_message = "nat_gateway_count must be 0, 1 or 3."
  }
}

variable "enable_flow_logs" {
  description = "VPC flow logs (traffic_type ALL) to the S3 bucket created here. Used by the kafka-crossaz-flowlogs drill."
  type        = bool
  default     = false
}
