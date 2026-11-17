variable "kafka_version" {
  type    = string
  default = "3.9.x"
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "broker_volume_gb" {
  type    = number
  default = 20
}

variable "scram_username" {
  type    = string
  default = "arena"
}
