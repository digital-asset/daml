#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

# Setup paths
# [start-docs-entry: offline root key proto image]
CURRENT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export BUF_PROTO_IMAGE="$CURRENT_DIR/../../scripts/offline-root-key/root_namespace_buf_image.bin"
source "$CURRENT_DIR/../../scripts/offline-root-key/utils.sh"
# [end-docs-entry: offline root key proto image]
