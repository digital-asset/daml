#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

is_test=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --test)
      shift
      is_test=1
      ;;
    *)
      echo "$0: unknown argument $1" >&2
      exit 1
      ;;
  esac
done

# These commands should be run at the root of the repo.
# We write backslash-colon instead of colon ro the grep does not pick up itself.

if [[ $is_test = 1 ]]; then
  git grep --line-number TEST_EVIDENCE\: | bazel run security:evidence-security | diff security-evidence.md -
else
  git grep --line-number TEST_EVIDENCE\: | bazel run security:evidence-security > security-evidence.md
fi
