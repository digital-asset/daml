#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $(basename $0) SNAPSHOT_CONFIG REPO_DIR SNAPSHOT_DIR" >&2
  echo "  - generate snapshot data for use in JMH benchmarking of Daml choices" >&2
  echo "SNAPSHOT_CONFIG: a JSON file defining the Dar file, Daml project build directory and scripts that should be used, for each Daml project to be profiled (JSON schema defined in snapshot-data.schema)." >&2
  echo "REPO_DIR: the directory holding the Daml project code to be snapshotted." >&2
  echo "SNAPSHOT_DIR: the directory to which snapshotting data will be saved." >&2
  exit 1
fi

SNAPSHOT_CONFIG=$1
REPO=$2
SNAPSHOT_DIR=$3

SIZE=$(cat "$SNAPSHOT_CONFIG" | jq "length")

for ((i=0;i<$SIZE;++i)); do
  DAR_NAME=$(cat "$SNAPSHOT_CONFIG" | jq -r ".[$i].name")
  DAR_DIR=$(cat "$SNAPSHOT_CONFIG" | jq -r ".[$i].dar_dir")
  SCRIPTS_SIZE=$(cat "$SNAPSHOT_CONFIG" | jq ".[$i].scripts|length")
  for ((j=0;j<$SCRIPTS_SIZE;++j)); do
    SCRIPT=$(cat "$SNAPSHOT_CONFIG" | jq -r ".[$i].scripts[$j]")
    DAR_FILE="$REPO/$DAR_DIR/$DAR_NAME" \
    SCRIPT_NAME="$SCRIPT" \
    SNAPSHOT_DIR="$SNAPSHOT_DIR" \
      bazel run --sandbox_debug //canton/community/daml-lf/snapshot:generate-snapshots || true
    DAR_FILE="$REPO/$DAR_DIR/$DAR_NAME" \
    SCRIPT_NAME="$SCRIPT" \
    SNAPSHOT_DIR="$SNAPSHOT_DIR" \
      bazel run --sandbox_debug //canton/community/daml-lf/snapshot:extract-snapshot-choices || true
  done
done

