data "aws_availability_zones" "available" {
  state = "available"
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = length(var.azs) > 0 ? var.azs : slice(data.aws_availability_zones.available.names, 0, 3)
  # /20 private subnets (4091 IPs each, pods use VPC-CNI IPs), /24 public subnets.
  private_cidrs = [for i in range(3) : cidrsubnet(var.cidr, 4, i)]
  public_cidrs  = [for i in range(3) : cidrsubnet(var.cidr, 8, 240 + i)]
  nat_count     = var.nat_gateway_count
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = var.name }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = var.name }
}

resource "aws_subnet" "public" {
  count                   = 3
  vpc_id                  = aws_vpc.this.id
  availability_zone       = local.azs[count.index]
  cidr_block              = local.public_cidrs[count.index]
  map_public_ip_on_launch = true
  tags = {
    Name                                = "${var.name}-public-${local.azs[count.index]}"
    "kubernetes.io/role/elb"            = "1"
    "kubernetes.io/cluster/${var.name}" = "shared"
    tier                                = "public"
  }
}

resource "aws_subnet" "private" {
  count             = 3
  vpc_id            = aws_vpc.this.id
  availability_zone = local.azs[count.index]
  cidr_block        = local.private_cidrs[count.index]
  tags = {
    Name                                = "${var.name}-private-${local.azs[count.index]}"
    "kubernetes.io/role/internal-elb"   = "1"
    "kubernetes.io/cluster/${var.name}" = "shared"
    tier                                = "private"
  }
}

resource "aws_eip" "nat" {
  count  = local.nat_count
  domain = "vpc"
  tags   = { Name = "${var.name}-nat-${count.index}" }
}

resource "aws_nat_gateway" "this" {
  count         = local.nat_count
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id
  tags          = { Name = "${var.name}-${count.index}" }
  depends_on    = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-public" }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count          = 3
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# One private route table per AZ so a per-AZ NAT (count = 3) works; with one NAT all point at it.
resource "aws_route_table" "private" {
  count  = 3
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.name}-private-${local.azs[count.index]}" }
}

resource "aws_route" "private_nat" {
  count                  = local.nat_count > 0 ? 3 : 0
  route_table_id         = aws_route_table.private[count.index].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[local.nat_count == 3 ? count.index : 0].id
}

resource "aws_route_table_association" "private" {
  count          = 3
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = aws_route_table.private[*].id
  tags              = { Name = "${var.name}-s3" }
}

# ---------------------------------------------------------------- S3 buckets

resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  suffix = random_id.suffix.hex
}

resource "aws_s3_bucket" "pg_backups" {
  bucket        = "${var.name}-pg-backups-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "pg_backups" {
  bucket = aws_s3_bucket.pg_backups.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "pg_backups" {
  bucket                  = aws_s3_bucket.pg_backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket" "flow_logs" {
  count         = var.enable_flow_logs ? 1 : 0
  bucket        = "${var.name}-flow-logs-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "flow_logs" {
  count  = var.enable_flow_logs ? 1 : 0
  bucket = aws_s3_bucket.flow_logs[0].id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "flow_logs" {
  count                   = var.enable_flow_logs ? 1 : 0
  bucket                  = aws_s3_bucket.flow_logs[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Flow logs delivering to S3 use the service-linked delivery; no bucket policy is needed
# when the caller owns the bucket (AWS adds the log-delivery policy automatically).
resource "aws_flow_log" "vpc" {
  count                    = var.enable_flow_logs ? 1 : 0
  vpc_id                   = aws_vpc.this.id
  traffic_type             = "ALL"
  log_destination_type     = "s3"
  log_destination          = aws_s3_bucket.flow_logs[0].arn
  max_aggregation_interval = 60
  # Fields include az-id and pkt-srcaddr so the drill can attribute cross-AZ bytes.
  log_format = "$${version} $${interface-id} $${srcaddr} $${dstaddr} $${srcport} $${dstport} $${protocol} $${bytes} $${start} $${end} $${action} $${az-id} $${pkt-srcaddr} $${pkt-dstaddr} $${subnet-id}"
}
