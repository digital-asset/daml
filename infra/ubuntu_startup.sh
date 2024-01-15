#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Agent startup script
set -euo pipefail

## Hardening

# Commit sepukku on failure
trap "shutdown -h now" EXIT

# replace the default nameserver to not use the metadata server
echo "nameserver 8.8.8.8" > /etc/resolv.conf

# delete self
rm -vf "$0"

## Install system dependencies
apt-get update -q
apt-get install -qy \
  curl sudo \
  bzip2 rsync \
  jq liblttng-ust0 libcurl4 libkrb5-3 zlib1g \
  git \
  netcat \
  apt-transport-https \
  software-properties-common

# Install dependencies for Chrome (to run Puppeteer tests on the gsg)
# list taken from: https://github.com/puppeteer/puppeteer/blob/a3d1536a6b6e282a43521bea28aef027a7133df8/docs/troubleshooting.md#chrome-headless-doesnt-launch-on-unix
# see https://github.com/digital-asset/daml/pull/5540 for context
apt-get install -qy \
    gconf-service \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm-dev \
    libgcc1 \
    libgconf-2-4 \
    libgdk-pixbuf2.0-0 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    ca-certificates \
    fonts-liberation \
    libappindicator1 \
    libnss3 \
    lsb-release \
    xdg-utils \
    wget

${gcp_logging}
#install docker
# BEGIN Installing Docker per https://docs.docker.com/engine/install/ubuntu/
apt-get -y install apt-transport-https \
                   ca-certificates \
                   curl \
                   gnupg \
                   lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get -y install docker-ce docker-ce-cli containerd.io
docker run --rm hello-world
# END Installing Docker

#Start docker daemon
systemctl enable docker

## Install the VSTS agent
groupadd --gid 3000 vsts
useradd \
  --create-home \
  --gid 3000 \
  --shell /bin/bash \
  --uid 3000 \
  vsts
#add docker group to user
usermod -aG docker vsts
# let vsts user mount/unmount cache folders
echo "/tmp/bazel_cache /home/vsts/.cache/bazel auto rw,user,exec" >> /etc/fstab
echo "/tmp/disk_cache /home/vsts/.bazel-cache auto rw,user,exec" >> /etc/fstab

CACHE_SCRIPT=/home/vsts/reset_caches.sh

cat <<'RESET_CACHES' > $CACHE_SCRIPT
#!/usr/bin/env bash

set -euo pipefail

reset_cache() {
    local file mount_point
    file=$1
    mount_point=$2

    echo "Cleaning up '$mount_point'..."
    if [ -d "$mount_point" ]; then
        for pid in $(pgrep -a -f bazel | awk '{print $1}'); do
            echo "Killing $pid..."
            kill -s KILL $pid
        done
        for pid in $(lsof $mount_point | sed 1d | awk '{print $2}' | sort -u); do
            echo "Killing $pid..."
            kill -s KILL $pid
        done
        if mount -l | grep $mount_point; then
            umount $mount_point
        fi
        rm -rf $mount_point
    fi

    rm -f $file
    truncate -s ${size}g $file
    mkfs.ext2 -E root_owner=$(id -u):$(id -g) $file
    mkdir -p $mount_point
    mount $mount_point
    echo "Done."
}

reset_cache /tmp/bazel_cache /home/vsts/.cache/bazel
reset_cache /tmp/disk_cache /home/vsts/.bazel-cache
RESET_CACHES
chown vsts:vsts $CACHE_SCRIPT
chmod +x $CACHE_SCRIPT

su --login vsts <<'AGENT_SETUP'
set -euo pipefail

VSTS_ACCOUNT=${vsts_account}
VSTS_POOL=${vsts_pool}
VSTS_TOKEN=${vsts_token}

mkdir -p ~/agent
cd ~/agent
echo 'assignment=${assignment}' > .capabilities

echo Determining matching VSTS agent...
VSTS_AGENT_RESPONSE=$(curl -sSfL \
  -u "user:$VSTS_TOKEN" \
  -H 'Accept:application/json;api-version=3.0-preview' \
  "https://$VSTS_ACCOUNT.visualstudio.com/_apis/distributedtask/packages/agent?platform=linux-x64")

VSTS_AGENT_URL=$(echo "$VSTS_AGENT_RESPONSE" \
  | jq -r '.value | map([.version.major,.version.minor,.version.patch,.downloadUrl]) | sort | .[length-1] | .[3]')

if [ -z "$VSTS_AGENT_URL" -o "$VSTS_AGENT_URL" == "null" ]; then
  echo 1>&2 error: could not determine a matching VSTS agent - check that account \'$VSTS_ACCOUNT\' is correct and the token is valid for that account
  exit 1
fi

echo Downloading and installing VSTS agent...
curl -sSfL "$VSTS_AGENT_URL" | tar -xz --no-same-owner

set +u
source ./env.sh
set -u

./config.sh \
  --acceptTeeEula \
  --agent "$(hostname)" \
  --auth PAT \
  --pool "$VSTS_POOL" \
  --replace \
  --token "$VSTS_TOKEN" \
  --unattended \
  --url "https://$VSTS_ACCOUNT.visualstudio.com"
AGENT_SETUP

## Hardening

chown --recursive root:root /home/vsts/agent/{*.sh,bin,externals}

## Install Nix

# This needs to run inside of a user with sudo access
echo "vsts ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/nix_installation
${nix}
rm /etc/sudoers.d/nix_installation

# Note: the "hydra.da-int.net" string is now part of the name of the key for
# legacy reasons; it bears no relation to the DNS hostname of the current
# cache.
cat <<NIX_CONF > /etc/nix/nix.conf
extra-substituters = https://nix-cache.da-ext.net
extra-trusted-public-keys = hydra.da-int.net-2:91tXuJGf/ExbAz7IWsMsxQ5FsO6lG/EGM5QVt+xhZu0= hydra.da-int.net-1:6Oy2+KYvI7xkAOg0gJisD7Nz/6m8CmyKMbWfSKUe03g=
build-users-group = nixbld
cores = 1
max-jobs = 0
sandbox = relaxed
NIX_CONF

systemctl restart nix-daemon

# Initialize caches
su --login vsts <<'CACHE_INIT'
# user-wide bazel disk cache override
echo "build:linux --disk_cache=~/.bazel-cache" > ~/.bazelrc
# set up cache folders
/home/vsts/reset_caches.sh
CACHE_INIT

# Remove /home/vsts/daml folder that might be present from cache warmup
rm -R /home/vsts/daml || true

## Finish

# run the fake local webserver, taken from the docker image
web-server() {
  while true; do
    printf 'HTTP/1.1 302 Found\r\nLocation: https://%s.visualstudio.com/_admin/_AgentPool\r\n\r\n' "${vsts_account}" | nc -l -p 80 -q 0 > /dev/null
  done
}
web-server &

# Start the VSTS agent
su --login vsts <<RUN
cd /home/vsts/agent

trap "./config.sh remove --auth PAT --unattended --token ${vsts_token}" EXIT

./run.sh
RUN
