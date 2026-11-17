output "cluster_name" { value = aws_eks_cluster.this.name }
output "cluster_endpoint" { value = aws_eks_cluster.this.endpoint }
output "cluster_version" { value = aws_eks_cluster.this.version }
output "region" { value = local.region }
output "pg_backup_bucket" { value = local.net.pg_backup_bucket }
output "node_group_names" { value = [for k, v in aws_eks_node_group.this : v.node_group_name] }
output "update_kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${local.region} --name ${aws_eks_cluster.this.name} --alias ${aws_eks_cluster.this.name}"
}
