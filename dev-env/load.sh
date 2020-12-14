#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Source this file to add all tools to your PATH.

# shellcheck source=./lib/ensure-nix
source "$(dirname "${BASH_SOURCE[0]}")/lib/ensure-nix"
# shellcheck disable=SC2016
PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && nix-shell --pure --run 'echo $PATH'):${PATH}"
export PATH
