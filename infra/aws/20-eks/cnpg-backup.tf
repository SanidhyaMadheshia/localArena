# Pod Identity for CloudNativePG backups (barman-cloud) to the S3 bucket from 10-network.
# CNPG names the pod service account after the Cluster: localarena/arena-db
# (infra/helm/localarena/templates/postgres.yaml, `kind: Cluster, name: arena-db`).
# The association can exist before the namespace or service account does.

resource "aws_iam_role" "cnpg_backup" {
  name               = "${local.name}-cnpg-backup"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

data "aws_iam_policy_document" "cnpg_backup" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.net.pg_backup_bucket_arn]
  }
  statement {
    sid = "ObjectRW"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.net.pg_backup_bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "cnpg_backup" {
  name   = "pg-backup-bucket"
  role   = aws_iam_role.cnpg_backup.id
  policy = data.aws_iam_policy_document.cnpg_backup.json
}

# arena-db-restore: the restore-from-S3 drill (scripts/drills/pg-restore-from-s3.sh) bootstraps a
# second CNPG cluster of that name, whose service account needs the same access.
resource "aws_eks_pod_identity_association" "cnpg_backup" {
  for_each        = toset(concat([var.cnpg_service_account], var.cnpg_extra_service_accounts))
  cluster_name    = aws_eks_cluster.this.name
  namespace       = var.cnpg_namespace
  service_account = each.key
  role_arn        = aws_iam_role.cnpg_backup.arn
  depends_on      = [aws_eks_addon.pod_identity_agent]
}
