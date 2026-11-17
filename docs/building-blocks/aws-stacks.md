# AWS stacks (burner account)

OpenTofu stacks and scripts that bring the lab up on one AWS account in one command. Local state per
stack, no remote backend. Everything below was validated offline only (`tofu validate`, `bash -n`,
`shellcheck`); see "Unverified" at the end.

## Layout and order

| Order | Path | Creates |
|---|---|---|
| 0 | `infra/aws/00-preflight/preflight.sh` | Nothing. Read-only PASS / WARN / BLOCKED table: identity, region, vCPU quota `L-1216C47A`, `m7i.large` offering per AZ (suggests `m6i.large`, `m5.large`), EKS / NAT / MSK / ECR / IAM / S3 access. |
| 1 | `infra/aws/10-network` | VPC `10.40.0.0/16`, 3 public (/24) + 3 private (/20) subnets over the first 3 AZs, IGW, `nat_gateway_count` NAT gateways (default 1), route tables, S3 gateway endpoint, ELB and cluster-discovery subnet tags, PG backup S3 bucket (random suffix, encrypted, public access blocked, `force_destroy`), optional VPC flow logs (`ALL`, S3, off by default). |
| 2 | `infra/aws/20-eks` | EKS cluster (API auth mode, access entry for the admin, API endpoint restricted to `admin_cidrs`, `audit` logs only), add-ons vpc-cni, coredns, kube-proxy, eks-pod-identity-agent, aws-ebs-csi-driver (Pod Identity role), one managed node group per AZ for `general` and `kafka` (6 groups, IMDSv2 required, hop limit 1, encrypted gp3 root), CNPG backup role with Pod Identity. |
| 2b | `infra/aws/manifests/gp3-storageclass.yaml` | Default `gp3` StorageClass (encrypted, `WaitForFirstConsumer`, expandable, `Retain`). Applied by `aws-up.sh`, which also removes the default flag from `gp2`. |
| 3 | `infra/aws/platform` | Symlinked copy of `infra/opentofu` without `imports.tf`, with its own state. CNPG operator, metrics-server, Argo CD. |
| 4 | ECR (script) | Repositories `localarena/api`, `localarena/gameserver`, `localarena/bots`, `localarena/streams`. |
| opt | `infra/aws/30-msk` | Provisioned MSK, 3 brokers, SASL/SCRAM, KMS key, `AmazonMSK_` secret and association. Only for experiment 015. |

Why `infra/aws/platform` exists: `infra/opentofu/imports.tf` adopts resources of the kind cluster. On a
fresh EKS cluster those `import` blocks fail ("cannot import non-existent remote object"), and import
blocks are not allowed in child modules. The symlink directory reuses `main.tf`, `variables.tf`,
`versions.tf`, `outputs.tf` and the committed lock file, so the platform code stays single-sourced and
the kind state stays untouched.

## Day 1

```bash
aws configure            # or AWS_PROFILE / SSO; use an IAM user or role, not the root user
export AWS_REGION=us-east-1
infra/aws/00-preflight/preflight.sh      # fix BLOCKED rows first
scripts/aws-up.sh --yes                  # about 20 to 30 min (EKS control plane ~10 min)
```

`aws-up.sh` flags: `--step network|eks|platform|ecr|all`, `--yes` (tofu `-auto-approve`). Env:
`AWS_REGION`, `ADMIN_CIDR` (default: your IP from `ifconfig.me` as `/32`), `ADMIN_PRINCIPAL_ARN`
(default: caller identity; assumed-role ARNs are converted to the role ARN; root is refused),
`TF_EXTRA_EKS` (extra tofu args, e.g. `-var=instance_type=m6i.large`), `SKIP_PREFLIGHT=1`.

If your IP changes, re-run `scripts/aws-up.sh --step eks --yes` to update `public_access_cidrs`.

## Variables that matter

| Stack | Variable | Default | Note |
|---|---|---|---|
| 10-network | `region`, `name` | us-east-1, `localarena` | `name` is the cluster name and must match in 20-eks (it is read from state). |
| 10-network | `nat_gateway_count` | 1 | 0, 1 or 3. One NAT is an egress SPOF for one AZ failure. |
| 10-network | `enable_flow_logs` | false | Needed for `kafka-crossaz-flowlogs`. |
| 20-eks | `admin_cidrs` | none, required | Rejects empty and `0.0.0.0/0`. |
| 20-eks | `admin_principal_arn` | none, required | IAM user or role ARN. |
| 20-eks | `kubernetes_version` | `1.36` | Newest on EKS is 1.37; default is newest minus one so drill 014 can upgrade. |
| 20-eks | `instance_type`, `kafka_instance_type` | `m7i.large` | Fallbacks `m6i.large`, `m5.large`. |
| 20-eks | `general_nodes_per_az`, `kafka_nodes_per_az` | 1, 1 | 0 scales that workload to zero. `node_max_per_az` (default 2) caps growth. |
| 20-eks | `capacity_type` | `ON_DEMAND` | `SPOT` is possible but not for Kafka drills. |
| 20-eks | `cnpg_service_account` | `arena-db` | CNPG names the service account after the Cluster (`infra/helm/localarena/templates/postgres.yaml`). `arena-db-restore` also gets access for the restore drill. |
| infra/opentofu | `insecure_kubelet_tls` | true | Passes `--kubelet-insecure-tls` to metrics-server. Keep true on kind, set false on EKS (`aws-up.sh` does). |

Each stack has a `terraform.tfvars.example`. Real `terraform.tfvars` files are gitignored.

## Scale to zero (overnight)

```bash
scripts/aws-scale-nodes.sh 0      # all node groups to 0; scripts/aws-scale-nodes.sh 1 to bring back
scripts/aws-scale-nodes.sh 0 --group general
```

The script changes desired size outside OpenTofu; the next `tofu apply` in `20-eks` resets it. For a
durable change set `general_nodes_per_az=0` and `kafka_nodes_per_az=0`. At zero nodes you still pay for
the EKS control plane ($0.10/h), the NAT gateway and the EBS volumes. Kafka and Postgres pods stay
Pending until nodes return; EBS volumes keep their data. On scale-up each AZ gets its node back, so
pinned volumes reattach.

## Teardown

```bash
scripts/aws-teardown.sh --yes            # add --purge to delete leftover tagged EBS volumes
```

Order: delete LoadBalancer Services and Ingresses, Strimzi and CNPG clusters, PVCs and PVs; wait; destroy
`20-eks`; destroy `10-network`; delete ECR repos with `--force`; remove the platform state file; then
report EBS volumes (tag filter), load balancers, ENIs, NAT gateways, Elastic IPs, ECR repos and the
EKS cluster as `clean` or `DIRTY`. Exit code 1 when dirty. `Retain` on the gp3 class means volumes
survive PVC deletion on purpose, so expect them in the report unless you pass `--purge`.
Destroy the `30-msk` stack first (`scripts/drills/msk-delete.sh`) if you created it: its security
group and subnets depend on `10-network`.

## Budget guard (optional)

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
aws budgets create-budget --account-id "$ACCOUNT" \
  --budget '{"BudgetName":"localarena-burner","BudgetLimit":{"Amount":"50","Unit":"USD"},"TimeUnit":"MONTHLY","BudgetType":"COST"}' \
  --notifications-with-subscribers '[{"Notification":{"NotificationType":"ACTUAL","ComparisonOperator":"GREATER_THAN","Threshold":80,"ThresholdType":"PERCENTAGE"},"Subscribers":[{"SubscriptionType":"EMAIL","Address":"you@example.com"}]}]'
```

Budgets lag by hours. They alert; they do not stop spending.

## Known risks

- **NACL isolation drills** (`kafka-az-isolate`, `kafka-two-az-isolate`) replace a subnet's network ACL with
  an empty one (deny all). Nodes there lose the EKS API, SSM and the NAT path. A trap restores the ACLs
  on exit; if the shell is killed run `scripts/drills/nacl-restore.sh <state-file>` (path is printed
  and stored in `experiments/011-kafka-resilience/results/`). Never run them while another drill runs.
- **Single NAT**: a NAT AZ failure cuts image pulls and ECR access for private nodes in the other AZs.
  ECR and S3 traffic would be cheaper through VPC endpoints; only the S3 gateway endpoint is created.
- **Retain StorageClass** leaves EBS volumes behind; teardown reports them. They bill until deleted.
- **Public API**: restricted to your /32. If your IP changes you lose kubectl until you re-apply.
- **IMDS hop limit 1** blocks pods from IMDS. Pods that need AWS access must use Pod Identity. Add-ons
  that use IRSA or IMDS (some third-party controllers) will fail with credentials errors.
- **Access entries**: the cluster creator gets no implicit admin (`bootstrap_cluster_creator_admin_permissions=false`).
  If `admin_principal_arn` is wrong you are locked out of Kubernetes until you fix the entry with the AWS API.
- **vCPU quota**: 6 nodes x 2 vCPU = 12. Drills 011g (extra broker) and 014 (replacement nodes) need headroom; ask for 24.
- **CNPG backup config**: `pg-restore-from-s3` patches `spec.backup` on `arena-db`, which the chart does
  not set. Argo CD self-heal would revert it. The in-tree barman support is deprecated (removed in CNPG 1.32).
- **Strimzi names**: drills assume Kafka `localarena` in namespace `data`, node pool `dual-role`, Streams
  deployment `streams` in `localarena` (label `app=streams`). Override with env vars (`KAFKA`, `KAFKA_NS`,
  `STREAMS_*`) if yours differ; the streams deployment is not in the chart in this checkout.

## Unverified (no AWS credentials were available)

Only offline checks were run. Not exercised: any AWS API call, `tofu plan/apply`, preflight against real
APIs (tested only with a stub `aws`), add-on/Pod Identity wiring, node group joins with the launch
template, flow log field format, MSK secret/KMS policy, and every drill script. Expect small fixes on day 1.
