#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Source this file to add all tools to your PATH.

PATH="$(nix-shell --pure --run 'echo $PATH'):${PATH}"
export PATH
