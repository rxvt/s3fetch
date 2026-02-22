#!/usr/bin/env bash
# =============================================================================
# Launch and configure an EC2 spot instance for running s3fetch benchmarks
#
# Usage (from repo root):
#   bash benchmarks/setup_ec2.sh
#
# Prerequisites (local machine):
#   - AWS CLI v2 configured with credentials for us-east-1
#   - SSM Session Manager plugin installed:
#       curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o ssm-plugin.deb
#       sudo dpkg -i ssm-plugin.deb
#
# When done benchmarking, terminate the instance:
#   aws ec2 terminate-instances --region us-east-1 --instance-ids <INSTANCE_ID>
# =============================================================================

set -euo pipefail

REGION="us-east-1"
INSTANCE_TYPE="t3.medium"
INSTANCE_PROFILE="S3FetchBenchmarkInstanceProfile"

# ---------------------------------------------------------------------------
# 1. Find latest Amazon Linux 2023 AMI
# ---------------------------------------------------------------------------
echo "Looking up latest Amazon Linux 2023 AMI..."
AMI=$(aws ec2 describe-images \
  --region "$REGION" \
  --owners amazon \
  --filters 'Name=name,Values=al2023-ami-2023*-x86_64' \
             'Name=state,Values=available' \
  --query 'sort_by(Images, &CreationDate)[-1].ImageId' \
  --output text)

echo "AMI: $AMI"

# ---------------------------------------------------------------------------
# 2. Launch spot instance
# ---------------------------------------------------------------------------
echo "Launching $INSTANCE_TYPE spot instance..."
INSTANCE_ID=$(aws ec2 run-instances \
  --region "$REGION" \
  --image-id "$AMI" \
  --instance-type "$INSTANCE_TYPE" \
  --instance-market-options 'MarketType=spot' \
  --iam-instance-profile "Name=$INSTANCE_PROFILE" \
  --metadata-options 'HttpTokens=required,HttpEndpoint=enabled' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=s3fetch-benchmark}]' \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "Instance ID: $INSTANCE_ID"

# ---------------------------------------------------------------------------
# 3. Wait for instance to be running
# ---------------------------------------------------------------------------
echo "Waiting for instance to enter running state..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"
echo "Instance is running. Waiting 30s for SSM agent to register..."
sleep 30

# ---------------------------------------------------------------------------
# 4. Build the remote setup script and send it via SSM
# ---------------------------------------------------------------------------
echo "Installing tools on instance via SSM..."
SETUP_COMMANDS=$(cat <<'REMOTE'
set -euo pipefail
echo "=== Installing tools ==="

# uv (installs to ~/.local/bin)
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
echo "uv: OK"

# s3fetch + s3cmd via uv tool install (isolated envs, on PATH via ~/.local/bin)
uv tool install s3fetch
uv tool install s3cmd
echo "s3fetch + s3cmd: OK"

# s5cmd
curl -sL https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_linux_amd64.tar.gz | tar xz
sudo mv s5cmd /usr/local/bin/
echo "s5cmd: OK"

# rclone
curl -s https://rclone.org/install.sh | sudo bash > /dev/null 2>&1
echo "rclone: OK"

# hyperfine
curl -sL https://github.com/sharkdp/hyperfine/releases/download/v1.18.0/hyperfine-v1.18.0-x86_64-unknown-linux-musl.tar.gz | tar xz
sudo mv hyperfine-v1.18.0-x86_64-unknown-linux-musl/hyperfine /usr/local/bin/
echo "hyperfine: OK"

# Configure rclone to use the instance role (no credentials needed)
rclone config create s3bench s3 provider AWS region us-east-1 env_auth true > /dev/null 2>&1
echo "rclone remote: OK"

# Clone the s3fetch repo
git clone --quiet https://github.com/rxvt/s3fetch.git ~/s3fetch
echo "s3fetch repo: OK"

echo ""
echo "=== Version check ==="
for tool in s3fetch aws s5cmd s3cmd rclone hyperfine uv; do
  echo -n "  $tool: "
  command -v "$tool" > /dev/null && echo "found" || echo "NOT FOUND"
done

echo ""
echo "=== Setup complete! ==="
echo ""
echo "To run benchmarks:"
echo "  cd ~/s3fetch && bash benchmarks/run_benchmarks.sh"
REMOTE
)

aws ssm send-command \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters "commands=[\"$SETUP_COMMANDS\"]" \
  --output text > /dev/null

# Wait for the setup command to complete
echo "Waiting for tool installation to complete (this takes ~60s)..."
sleep 90

# ---------------------------------------------------------------------------
# 5. Connect
# ---------------------------------------------------------------------------
echo ""
echo "================================================================="
echo "  Instance ready!"
echo "  Instance ID: $INSTANCE_ID"
echo ""
echo "  To connect now:"
echo "    aws ssm start-session --region $REGION --target $INSTANCE_ID"
echo ""
echo "  Once connected, run:"
echo "    cd ~/s3fetch && bash benchmarks/run_benchmarks.sh"
echo ""
echo "  To terminate when done:"
echo "    aws ec2 terminate-instances --region $REGION --instance-ids $INSTANCE_ID"
echo "================================================================="
echo ""

# Connect automatically
aws ssm start-session --region "$REGION" --target "$INSTANCE_ID"
