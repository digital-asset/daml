#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

BUF_IMAGE_TMPDIR="$(mktemp -d)"
trap 'rm -rf ${BUF_IMAGE_TMPDIR}' EXIT

LATEST_STABLE_TAG="$(git tag --no-merged | grep -v "snapshot" | sort -V | tail -1)"
echo "Checking protobuf against tag '${LATEST_STABLE_TAG}' (i.e., the most recent stable tag)"
git checkout "${LATEST_STABLE_TAG}"

BUF_IMAGE="${BUF_IMAGE_TMPDIR}/buf.bin"
buf build -o "${BUF_IMAGE}"
git checkout main
buf breaking --against "${BUF_IMAGE}"
