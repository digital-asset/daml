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

# Nix cache downloads can sometimes be flaky and end with "unexpected end-of-file" so we
# repeat this a few times. There does not seem to be an option that we can pass to nix
# to make it retry itself. See https://github.com/NixOS/nix/issues/2794 for the issue requesting
# this feature.
NIX_FAILED=0
for i in `seq 10`; do
    NIX_FAILED=0
    nix-build nix -A tools -A cached 2>&1 | tee nix_log || NIX_FAILED=1
    # It should be in the last line but let’s use the last 3 and wildcards
    # to be robust against slight changes.
    if [[ $NIX_FAILED -ne 0 ]] && [[ $(tail -n 3 nix_log) == *"unexpected end-of-file"* ]]; then
        echo "Restarting nix-build due to failed cache download"
        continue
    fi
    break
done
if [[ $NIX_FAILED -ne 0 ]]; then
    exit 1
fi
