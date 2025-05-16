#!/usr/bin/env bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail  # Exit on error, prevent unset vars, fail pipeline on first error

source "$(dirname "$0")/utils.sh"

OUTPUT_DIR="${OUTPUT_DIR:=$(mktemp -d)}"
echo "Using $OUTPUT_DIR as temporary directory to write files to."
# [start-docs-entry: script variables]
# Points to the location of the offline root key scripts under the scripts/offline-root-key directory in the release artifact
SCRIPTS_ROOT="$(dirname "$0")/../../scripts/offline-root-key"
# This script assumes the root namespace key has already been generated
PRIVATE_KEY="$1"
# Path to file containing the namespace delegation to be revoked
CANTON_NAMESPACE_DELEGATION_TO_REVOKE="$2"
REVOKED_DELEGATION_PREFIX="$OUTPUT_DIR/revoked_delegation"
# [end-docs-entry: script variables]

# Prepare certificates
# [start-docs-entry: prepare revoked key cert]
"$SCRIPTS_ROOT/prepare-certs.sh" --revoke-delegation "$CANTON_NAMESPACE_DELEGATION_TO_REVOKE" --output "$REVOKED_DELEGATION_PREFIX"
# [end-docs-entry: prepare revoked key cert]

# Sign hash
# [start-docs-entry: sign revoked key cert]
openssl pkeyutl -rawin -inkey "$PRIVATE_KEY" -keyform DER -sign < "$REVOKED_DELEGATION_PREFIX.hash" > "$REVOKED_DELEGATION_PREFIX.signature"
# [end-docs-entry: sign revoked key cert]

# Assemble signature and transaction
# [start-docs-entry: assemble revoked key cert]
"$SCRIPTS_ROOT/assemble-cert.sh" --prepared-transaction "$REVOKED_DELEGATION_PREFIX.prep" --signature "$REVOKED_DELEGATION_PREFIX.signature" --signature-algorithm ecdsa256 --output "$REVOKED_DELEGATION_PREFIX.cert"
# [end-docs-entry: assemble revoked key cert]
