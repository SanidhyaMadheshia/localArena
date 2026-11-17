# Optional MSK provisioned cluster (3 brokers, one per private subnet) with SASL/SCRAM.
# MSK takes about 25 to 40 minutes to create and bills hourly; destroy it right after experiment 015.

data "terraform_remote_state" "network" {
  backend = "local"
  config  = { path = "${path.module}/../10-network/terraform.tfstate" }
}

locals {
  net    = data.terraform_remote_state.network.outputs
  region = local.net.region
  name   = "${local.net.name}-msk"
}

resource "aws_security_group" "msk" {
  name        = local.name
  description = "MSK SASL/SCRAM and TLS from inside the VPC"
  vpc_id      = local.net.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "scram" {
  security_group_id = aws_security_group.msk.id
  cidr_ipv4         = local.net.vpc_cidr
  from_port         = 9096
  to_port           = 9096
  ip_protocol       = "tcp"
  description       = "SASL/SCRAM over TLS"
}

resource "aws_kms_key" "msk" {
  description             = "${local.name} (MSK and SCRAM secret; Secrets Manager default key is not allowed for MSK)"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "random_password" "scram" {
  length  = 32
  special = false
}

# MSK requires the secret name to start with AmazonMSK_ and a customer-managed KMS key.
resource "aws_secretsmanager_secret" "scram" {
  name                    = "AmazonMSK_${local.name}_${var.scram_username}"
  kms_key_id              = aws_kms_key.msk.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "scram" {
  secret_id     = aws_secretsmanager_secret.scram.id
  secret_string = jsonencode({ username = var.scram_username, password = random_password.scram.result })
}

data "aws_iam_policy_document" "scram_secret" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:getSecretValue"]
    resources = [aws_secretsmanager_secret.scram.arn]
    principals {
      type        = "Service"
      identifiers = ["kafka.amazonaws.com"]
    }
  }
}

resource "aws_secretsmanager_secret_policy" "scram" {
  secret_arn = aws_secretsmanager_secret.scram.arn
  policy     = data.aws_iam_policy_document.scram_secret.json
}

resource "aws_msk_cluster" "this" {
  cluster_name           = local.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = local.net.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info { volume_size = var.broker_volume_gb }
    }
  }

  client_authentication {
    sasl { scram = true }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}

resource "aws_msk_scram_secret_association" "this" {
  cluster_arn     = aws_msk_cluster.this.arn
  secret_arn_list = [aws_secretsmanager_secret.scram.arn]
  depends_on      = [aws_secretsmanager_secret_version.scram, aws_secretsmanager_secret_policy.scram]
}
