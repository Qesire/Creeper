#!/usr/bin/env bash
set -euo pipefail

REGION="${CREEPER_AWS_REGION:-us-east-1}"
NAME="${CREEPER_VM_NAME:-creeper-aws-t4g-01}"
INSTANCE_TYPE="${CREEPER_AWS_INSTANCE_TYPE:-t4g.small}"
SUBNET_ID="${CREEPER_AWS_SUBNET_ID:-}"
SECURITY_GROUP_ID="${CREEPER_AWS_SECURITY_GROUP_ID:-}"
KEY_NAME="${CREEPER_AWS_KEY_NAME:-}"
APPLY="${CREEPER_APPLY:-0}"

command -v aws >/dev/null 2>&1 || {
  echo "AWS CLI is required" >&2
  exit 2
}
for pair in   "CREEPER_AWS_SUBNET_ID:$SUBNET_ID"   "CREEPER_AWS_SECURITY_GROUP_ID:$SECURITY_GROUP_ID"   "CREEPER_AWS_KEY_NAME:$KEY_NAME"; do
  name="${pair%%:*}"
  value="${pair#*:}"
  [[ -n "$value" ]] || {
    echo "set $name" >&2
    exit 2
  }
done

case "$INSTANCE_TYPE" in
  t3.micro|t3.small|t4g.micro|t4g.small|c7i-flex.large|m7i-flex.large) ;;
  *)
    echo "instance type is outside the post-2025 AWS Free Tier eligible list" >&2
    exit 2
    ;;
esac

ARCH=amd64
[[ "$INSTANCE_TYPE" == t4g.* ]] && ARCH=arm64
AMI_ID="$(aws ec2 describe-images   --region "$REGION"   --owners 099720109477   --filters     "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-$ARCH-server-*"     "Name=state,Values=available"   --query 'sort_by(Images,&CreationDate)[-1].ImageId'   --output text)"
[[ "$AMI_ID" != "None" && -n "$AMI_ID" ]] || {
  echo "failed to resolve Canonical Ubuntu 24.04 AMI" >&2
  exit 2
}

CMD=(
  aws ec2 run-instances
  --region "$REGION"
  --image-id "$AMI_ID"
  --instance-type "$INSTANCE_TYPE"
  --subnet-id "$SUBNET_ID"
  --security-group-ids "$SECURITY_GROUP_ID"
  --key-name "$KEY_NAME"
  --associate-public-ip-address
  --block-device-mappings
  "DeviceName=/dev/sda1,Ebs={VolumeSize=20,VolumeType=gp3,DeleteOnTermination=true}"
  --tag-specifications
  "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=Project,Value=Creeper}]"
)

printf 'AWS Free plan for accounts created on/after 2025-07-15 is credit/time limited, not perpetual. Public IPv4 and compute consume credits.\n' >&2
printf 'Resolved AMI: %s\nCommand:' "$AMI_ID"
printf ' %q' "${CMD[@]}"
printf '\n'

if [[ "$APPLY" == "1" ]]; then
  [[ "${CREEPER_ALLOW_AWS_CREDIT_SPEND:-0}" == "1" ]] || {
    echo "refusing apply: set CREEPER_ALLOW_AWS_CREDIT_SPEND=1 after reviewing current credits/costs" >&2
    exit 3
  }
  "${CMD[@]}"
else
  echo "dry-run only; set CREEPER_APPLY=1 and CREEPER_ALLOW_AWS_CREDIT_SPEND=1 to execute"
fi
