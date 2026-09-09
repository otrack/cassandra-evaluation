#!/bin/bash
#
# User-data handed to every benchmark instance through run-instances, executed
# once by cloud-init as root on first boot. Injects the operator's SSH key,
# installs Docker and the tooling the experiments need on the containers'
# host, then drops a marker file that infra_provision polls for.
#
# EC2 has no metadata-driven key-injection mechanism the way some other
# clouds' guest agents provide, so this script is the only hook that runs
# before an operator can reach the instance at all -- _aws_render_user_data
# splices the real key in over the placeholder below before this is
# base64-encoded for --user-data.

set -e

MARKER=/var/lib/bench-node-ready

if [ -f "${MARKER}" ]; then
    exit 0
fi

mkdir -p /home/ubuntu/.ssh
echo "__SSH_PUBLIC_KEY__" >> /home/ubuntu/.ssh/authorized_keys
chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 700 /home/ubuntu/.ssh
chmod 600 /home/ubuntu/.ssh/authorized_keys

export DEBIAN_FRONTEND=noninteractive

apt-get update -y
# The experiments run tc inside the containers (docker exec), so the images --
# not this host -- supply the binary they need.  iproute2 is installed here
# only so that a human can inspect or clear the instance's qdiscs by hand:
# under host networking a rule added by a container is applied to the
# instance's own interface and outlives that container.
apt-get install -y ca-certificates curl iproute2

if ! command -v docker >/dev/null 2>&1; then
    curl -fsSL https://get.docker.com | sh
fi

# Created ahead of any human user so that adding them to it later is enough.
groupadd -f docker

systemctl enable --now docker

# The benchmark starts long-lived JVMs; raise the limits Cassandra complains
# about rather than leaving warnings in every log.
cat > /etc/security/limits.d/99-bench.conf <<'EOF'
*  soft  nofile  1048576
*  hard  nofile  1048576
*  soft  memlock unlimited
*  hard  memlock unlimited
EOF

sysctl -w vm.max_map_count=1048575 >/dev/null
echo "vm.max_map_count=1048575" > /etc/sysctl.d/99-bench.conf

touch "${MARKER}"
