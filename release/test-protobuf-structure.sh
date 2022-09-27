#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

# --- begin runfiles.bash initialization ---
# Copy-pasted from Bazel's Bash runfiles library (tools/bash/runfiles/runfiles.bash).
if [[ ! -d "${RUNFILES_DIR:-/dev/null}" && ! -f "${RUNFILES_MANIFEST_FILE:-/dev/null}" ]]; then
  if [[ -f "$0.runfiles_manifest" ]]; then
    export RUNFILES_MANIFEST_FILE="$0.runfiles_manifest"
  elif [[ -f "$0.runfiles/MANIFEST" ]]; then
    export RUNFILES_MANIFEST_FILE="$0.runfiles/MANIFEST"
  elif [[ -f "$0.runfiles/bazel_tools/tools/bash/runfiles/runfiles.bash" ]]; then
    export RUNFILES_DIR="$0.runfiles"
  fi
fi
if [[ -f "${RUNFILES_DIR:-/dev/null}/bazel_tools/tools/bash/runfiles/runfiles.bash" ]]; then
  source "${RUNFILES_DIR}/bazel_tools/tools/bash/runfiles/runfiles.bash"
elif [[ -f "${RUNFILES_MANIFEST_FILE:-/dev/null}" ]]; then
  source "$(grep -m1 "^bazel_tools/tools/bash/runfiles/runfiles.bash " \
            "$RUNFILES_MANIFEST_FILE" | cut -d ' ' -f 2-)"
else
  echo >&2 "ERROR: cannot find @bazel_tools//tools/bash/runfiles:runfiles.bash"
  exit 1
fi
# --- end runfiles.bash initialization ---

PROTOS=$(rlocation $TEST_WORKSPACE/$1)
DIFF=$2
SORT=$3

export LANG=C

$DIFF -u <(unzip -Z1 $PROTOS | $SORT | sed 's|^[^/]*/||g') <(cat <<EOF
com/daml/daml_lf_1_14/daml_lf.proto
com/daml/daml_lf_1_14/daml_lf_1.proto
com/daml/daml_lf_1_15/daml_lf.proto
com/daml/daml_lf_1_15/daml_lf_1.proto
com/daml/daml_lf_dev/daml_lf.proto
com/daml/daml_lf_dev/daml_lf_1.proto
com/daml/ledger/api/v1/active_contracts_service.proto
com/daml/ledger/api/v1/admin/config_management_service.proto
com/daml/ledger/api/v1/admin/metering_report_service.proto
com/daml/ledger/api/v1/admin/object_meta.proto
com/daml/ledger/api/v1/admin/package_management_service.proto
com/daml/ledger/api/v1/admin/participant_pruning_service.proto
com/daml/ledger/api/v1/admin/party_management_service.proto
com/daml/ledger/api/v1/admin/user_management_service.proto
com/daml/ledger/api/v1/command_completion_service.proto
com/daml/ledger/api/v1/command_service.proto
com/daml/ledger/api/v1/command_submission_service.proto
com/daml/ledger/api/v1/commands.proto
com/daml/ledger/api/v1/completion.proto
com/daml/ledger/api/v1/contract_metadata.proto
com/daml/ledger/api/v1/event.proto
com/daml/ledger/api/v1/experimental_features.proto
com/daml/ledger/api/v1/ledger_configuration_service.proto
com/daml/ledger/api/v1/ledger_identity_service.proto
com/daml/ledger/api/v1/ledger_offset.proto
com/daml/ledger/api/v1/package_service.proto
com/daml/ledger/api/v1/testing/time_service.proto
com/daml/ledger/api/v1/transaction.proto
com/daml/ledger/api/v1/transaction_filter.proto
com/daml/ledger/api/v1/transaction_service.proto
com/daml/ledger/api/v1/value.proto
com/daml/ledger/api/v1/version_service.proto
EOF
)
