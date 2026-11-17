output "cluster_arn" { value = aws_msk_cluster.this.arn }
output "bootstrap_brokers_sasl_scram" { value = aws_msk_cluster.this.bootstrap_brokers_sasl_scram }
output "scram_secret_arn" { value = aws_secretsmanager_secret.scram.arn }
output "scram_username" { value = var.scram_username }
