#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# shellcheck disable=SC2174

# Installs nix on a fresh machine
set -euo pipefail

## Functions ##

step() {
  echo "step: $*" >&2
}

## Main ##

cd "$(dirname "$0")/.."

if [[ ! -e /nix ]]; then
  step "Installing Nix"

  sudo mkdir -m 0755 /nix
  sudo chown "$(id -u):$(id -g)" /nix

  # 2.2.2 seems to segfault on MacOS in CI so for now we use 2.2.1.
  curl -sfL https://nixos.org/releases/nix/nix-2.2.1/install | bash
fi

# shellcheck source=../dev-env/lib/ensure-nix
source dev-env/lib/ensure-nix

export NIX_CONF_DIR=$PWD/dev-env/etc

step "Building dev-env dependencies"
nix-build nix -A tools -A cached
