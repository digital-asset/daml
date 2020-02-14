#!/usr/bin/env bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Agent startup script
set -euo pipefail

## Hardening

# Commit harakiri on failure
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
  jq liblttng-ust0 libcurl3 libkrb5-3 libicu55 zlib1g \
  git \
  netcat \
  apt-transport-https \
  software-properties-common

curl -sSL https://dl.google.com/cloudagents/install-logging-agent.sh | bash

#install docker
DOCKER_VERSION="5:18.09.5~3-0~ubuntu-$(lsb_release -cs)"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -qy docker-ce=$DOCKER_VERSION docker-ce-cli=$DOCKER_VERSION containerd.io

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

su --login vsts <<'AGENT_SETUP'
set -euo pipefail

VSTS_ACCOUNT=${vsts_account}
VSTS_POOL=${vsts_pool}
VSTS_TOKEN=${vsts_token}

mkdir -p ~/agent
cd ~/agent

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
su --command "sh <(curl https://nixos.org/nix/install) --daemon" --login vsts
rm /etc/sudoers.d/nix_installation

# Note: the "hydra.da-int.net" string is now part of the name of the key for
# legacy reasons; it bears no relation to the DNS hostname of the current
# cache.
cat <<NIX_CONF > /etc/nix/nix.conf
binary-cache-public-keys = hydra.da-int.net-1:6Oy2+KYvI7xkAOg0gJisD7Nz/6m8CmyKMbWfSKUe03g= cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= hydra.nixos.org-1:CNHJZBh9K4tP3EKF6FkkgeVYsS3ohTl+oS0Qa8bezVs=
binary-caches = https://nix-cache.da-ext.net https://cache.nixos.org
build-users-group = nixbld
cores = 1
max-jobs = 0
sandbox = relaxed
NIX_CONF

systemctl restart nix-daemon

# Warm up local caches by building dev-env and current daml master
# This is allowed to fail, as we still want to have CI machines
# around, even when their caches are only warmed up halfway
su --login vsts <<'CACHE_WARMUP'
# user-wide bazel disk cache override

# clone and build
(
  git clone https://github.com/digital-asset/daml
  cd daml
  git checkout ci-perf-test-playground
  ./ci/dev-env-install.sh
  ./build.sh "_$(uname)"
) || true
CACHE_WARMUP

# Purge old agents
su --login vsts <<'PURGE_OLD_AGENTS'
cd daml && \
VSTS_ACCOUNT=${vsts_account} VSTS_POOL=${vsts_pool} VSTS_TOKEN=${vsts_token} ./ci/azure-cleanup/purge_old_agents.py || true
PURGE_OLD_AGENTS

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
su --login --command "cd /home/vsts/agent && exec ./run.sh" - vsts
