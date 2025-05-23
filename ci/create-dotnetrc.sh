#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

if [ -z "${GITHUB_TOKEN}" ]; then
  echo "Error: The GITHUB_TOKEN environment variable is not set or is empty." >&2
  echo "Please set it before running this script." >&2
  exit 1
fi

NETRC_PATH="${HOME}/.netrc"

NETRC_CONTENT=$(cat <<EOF
machine api.github.com
    password ${GITHUB_TOKEN}
EOF
)

echo "${NETRC_CONTENT}" > "${NETRC_PATH}"
chmod 600 "${NETRC_PATH}"

