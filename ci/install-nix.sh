#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Installs nix on a fresh machine
set -exuo pipefail

sudo mkdir -p /nix
sudo chown "$(id -u):$(id -g)" /nix
curl -sfL https://nixos.org/nix/install | bash
