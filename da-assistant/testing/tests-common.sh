#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


log() { printf '%s\n' "$*"; }
error() { log "ERROR: $*" >&2; }
fatal() { error "$@"; exit 1; }

# appends a command to a trap
#
# - 1st arg:  code to add
# - remaining args:  names of traps to modify
#
trap_add() {
    trap_add_cmd=$1; shift || fatal "${FUNCNAME} usage error"
    new_cmd=
    for trap_add_name in "$@"; do
        # Grab the currently defined trap commands for this trap
        existing_cmd=`trap -p "${trap_add_name}" |  awk -F"'" '{print $2}'`

        # Define default command
        if [[ -z "${existing_cmd}" ]]; then
            new_cmd="${trap_add_cmd}"
        else
            # LIFO
            new_cmd="${trap_add_cmd}; ${existing_cmd}"
        fi

        # Generate the new command

        # Assign the test
        trap "${new_cmd}" "${trap_add_name}" || \
            fatal "unable to add to trap ${trap_add_name}"
    done
}

TESTROOT=$(mktemp -d)

function cleanup() {
  echo "Cleaning up $TESTROOT"
  rm -rf "$TESTROOT"
}
trap_add cleanup EXIT

# Build the installer
echo
echo "Building the SDK assistant:"
bazel build //da-assistant:install.run.0
cp $(bazel info bazel-genfiles)/da-assistant/installer.run.0 $TESTROOT/da.run
chmod +x $TESTROOT/da.run

cat > $TESTROOT/setup-input <<EOF
test@digitalasset.com
EOF
