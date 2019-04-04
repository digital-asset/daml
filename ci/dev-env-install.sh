#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Installs nix on a fresh machine
set -exuo pipefail

## Functions ##

step() {
  echo "step: $*" >&2
}

## Main ##

cd "$(dirname "$0")/.."

step "Installing Nix"

sudo mkdir -p /nix
sudo chown "$(id -u):$(id -g)" /nix
curl -sfL https://nixos.org/nix/install | bash

# shellcheck source=../dev-env/lib/ensure-nix
source dev-env/lib/ensure-nix

step "Building dev-env dependencies"
nix-build nix -A tools -A cached
