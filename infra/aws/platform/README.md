# platform (symlinked view of infra/opentofu)

`infra/opentofu` carries `imports.tf` (adopts the kind cluster's pre-existing resources). Those
`import` blocks fail on a fresh EKS cluster ("cannot import non-existent remote object"), and import
blocks are not allowed in child modules. This directory symlinks every file of `infra/opentofu`
except `imports.tf`, so EKS gets the same platform code with its own state
(`infra/aws/platform/terraform.tfstate`, gitignored) and the kind state stays untouched.

`../kubernetes` is a symlink so `${path.module}/../kubernetes/helm/cnpg-values.yaml` resolves.

Run through `scripts/aws-up.sh --step platform`, which passes `kube_context`,
`insecure_kubelet_tls=false`, `manage_app=false`.
