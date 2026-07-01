#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Fails if docs/source/ or docs/manually-written/ changed without a
# corresponding update to docs/sharable/.  The CI job
# ci/verify-docs-synchronized.sh rebuilds sharable-docs from those inputs and
# diffs against docs/sharable/; this hook catches the mismatch early.

set -euo pipefail

if [[ -n "${DADE_SKIP_DOCS_SYNC_CHECK:-}" ]]; then
  exit 0
fi

SHARABLE_CHANGED=$(git diff --cached --name-only -- sdk/docs/sharable/)

if [[ -z "$SHARABLE_CHANGED" ]]; then
  echo "
Files under docs/source/ or docs/manually-written/ were modified without
updating docs/sharable/.  The CI job 'verify-docs-synchronized' will fail.

Run:
    ci/synchronize-docs.sh

and stage the result, or \`export DADE_SKIP_DOCS_SYNC_CHECK=1\` and try again.
" >&2
  exit 1
fi
