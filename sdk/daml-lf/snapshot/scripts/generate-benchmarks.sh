#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $(basename $0) SNAPSHOT_CONFIG REPO_DIR SNAPSHOT_DIR" >&2
  echo "  - generate JMH benchmarking data from snapshot files and save Daml choice timing results as JSON" >&2
  echo "SNAPSHOT_CONFIG: a JSON file defining the Dar file, Daml project build directory and scripts that should be used, for each Daml project to be profiled (JSON schema defined in snapshot-data.schema)." >&2
  echo "REPO_DIR: the directory holding the Daml project code to be JMH benchmarked." >&2
  echo "SNAPSHOT_DIR: the directory holding snapshotting data to be used during JMH benchmarking." >&2
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
    SCRIPT_NAME=$(cat "$SNAPSHOT_CONFIG" | jq -r ".[$i].scripts[$j]")
    SCRIPT_FUNC=$(echo "$SCRIPT_NAME" | cut -d ":" -f 2)
    if [ -f "$SNAPSHOT_DIR/$DAR_NAME/$SCRIPT_FUNC"/snapshot-participant0*.bin ]; then
      ENTRIES_FILE=$(ls "$SNAPSHOT_DIR/$DAR_NAME/$SCRIPT_FUNC"/snapshot-participant0*.bin)
      CHOICES_FILE=$(ls "$SNAPSHOT_DIR/$DAR_NAME/$SCRIPT_FUNC"/snapshot-participant0*.bin.choices)
      for CHOICE in $(cat "$CHOICES_FILE"); do
        MODULE_NAME=$(echo "${CHOICE}" | cut -d ":" -f 2)
        TEMPLATE_NAME=$(echo "${CHOICE}" | cut -d ":" -f 3)
        CHOICE_NAME=$(echo "${CHOICE}" | cut -d ":" -f 4)
        bazel run --sandbox_debug //daml-lf/snapshot:replay-benchmark -- \
          -p entriesFile="$ENTRIES_FILE" \
          -p choiceName="$MODULE_NAME:$TEMPLATE_NAME:$CHOICE_NAME" \
          -p darFile="$REPO/$DAR_DIR/$DAR_NAME" \
          -rff "$SNAPSHOT_DIR/$DAR_NAME/$SCRIPT_FUNC/jmh-result-$CHOICE_NAME.json" \
          -rf json || true
      done
    else
      echo "Skipping benchmarking for $DAR_NAME/$SCRIPT_FUNC as no snapshot data has been saved"
    fi
  done
done

