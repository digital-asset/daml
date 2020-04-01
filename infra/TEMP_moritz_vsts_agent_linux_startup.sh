#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

trap - EXIT
