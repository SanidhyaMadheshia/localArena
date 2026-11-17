data "terraform_remote_state" "network" {
  backend = "local"
  config  = { path = "${path.module}/../10-network/terraform.tfstate" }
}

locals {
  net        = data.terraform_remote_state.network.outputs
  region     = local.net.region
  name       = local.net.name
  azs        = local.net.azs
  subnet_ids = local.net.private_subnet_ids
  az_subnets = { for i, az in local.azs : az => local.subnet_ids[i] }
}

# ---------------------------------------------------------------- cluster

resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${local.name}/cluster"
  retention_in_days = 3
}

resource "aws_iam_role" "cluster" {
  name = "${local.name}-eks-cluster"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow", Action = "sts:AssumeRole"
      Principal = { Service = "eks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  name                          = local.name
  version                       = var.kubernetes_version
  role_arn                      = aws_iam_role.cluster.arn
  enabled_cluster_log_types     = var.cluster_log_types
  bootstrap_self_managed_addons = true # EKS installs vpc-cni/kube-proxy/coredns; the add-ons below adopt them

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = false # admin comes from the explicit access entry below
  }

  upgrade_policy {
    support_type = "STANDARD" # avoid extended-support surcharge
  }

  vpc_config {
    subnet_ids              = local.subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.admin_cidrs
  }

  depends_on = [aws_iam_role_policy_attachment.cluster, aws_cloudwatch_log_group.cluster]
}

resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = var.admin_principal_arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "admin" {
  cluster_name  = aws_eks_cluster.this.name
  principal_arn = var.admin_principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope { type = "cluster" }
  depends_on = [aws_eks_access_entry.admin]
}

# ---------------------------------------------------------------- nodes

resource "aws_iam_role" "node" {
  name = "${local.name}-eks-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow", Action = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy",
    "AmazonEKS_CNI_Policy",
    "AmazonEC2ContainerRegistryReadOnly",
    "AmazonSSMManagedInstanceCore", # shell on nodes through SSM, no SSH or key pair
  ])
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/${each.key}"
}

# IMDSv2 required, hop limit 1: containers on the pod network cannot reach IMDS, so pods
# cannot steal the node role. Pods get AWS credentials through EKS Pod Identity instead.
# (hostNetwork pods such as aws-node still can.)
resource "aws_launch_template" "node" {
  name_prefix = "${local.name}-node-"

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_type           = "gp3"
      volume_size           = var.node_disk_gb
      encrypted             = true
      delete_on_termination = true
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags          = { project = "localarena", cluster = local.name }
  }
  tag_specifications {
    resource_type = "volume"
    tags          = { project = "localarena", cluster = local.name }
  }
}

locals {
  groups = {
    general = {
      instance_type = var.instance_type
      desired       = var.general_nodes_per_az
      taints        = []
    }
    kafka = {
      instance_type = coalesce(var.kafka_instance_type, var.instance_type)
      desired       = var.kafka_nodes_per_az
      taints        = [{ key = "dedicated", value = "kafka", effect = "NO_SCHEDULE" }]
    }
  }
  # One managed node group per (workload, AZ): each AZ is guaranteed its own node and
  # EBS-bound pods can be pinned to a zone.
  node_groups = merge([
    for g, cfg in local.groups : {
      for az, subnet in local.az_subnets : "${g}-${az}" => merge(cfg, { workload = g, az = az, subnet = subnet })
    }
  ]...)
}

resource "aws_eks_node_group" "this" {
  for_each        = local.node_groups
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${local.name}-${each.key}"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = [each.value.subnet]
  instance_types  = [each.value.instance_type]
  capacity_type   = var.capacity_type
  ami_type        = "AL2023_x86_64_STANDARD"

  launch_template {
    id      = aws_launch_template.node.id
    version = aws_launch_template.node.latest_version
  }

  scaling_config {
    desired_size = each.value.desired
    min_size     = 0
    max_size     = max(var.node_max_per_az, each.value.desired)
  }

  update_config { max_unavailable = 1 }

  labels = { workload = each.value.workload }

  dynamic "taint" {
    for_each = each.value.taints
    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }

  tags = { workload = each.value.workload, az = each.value.az }

  depends_on = [
    aws_iam_role_policy_attachment.node,
    aws_eks_addon.vpc_cni,
    aws_eks_addon.kube_proxy,
  ]
}

# ---------------------------------------------------------------- add-ons

resource "aws_eks_addon" "vpc_cni" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "vpc-cni"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_addon" "kube_proxy" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "kube-proxy"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

resource "aws_eks_addon" "pod_identity_agent" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "eks-pod-identity-agent"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
}

# CoreDNS and the EBS CSI driver run as pods, so they need nodes before they turn ACTIVE.
resource "aws_eks_addon" "coredns" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "coredns"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"
  depends_on                  = [aws_eks_node_group.this]
}

data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "${local.name}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = "aws-ebs-csi-driver"
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  pod_identity_association {
    role_arn        = aws_iam_role.ebs_csi.arn
    service_account = "ebs-csi-controller-sa"
  }

  depends_on = [aws_eks_node_group.this, aws_eks_addon.pod_identity_agent, aws_iam_role_policy_attachment.ebs_csi]
}
