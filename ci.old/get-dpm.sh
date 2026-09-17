#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# This file is no longer used to acquire DPM, but must still exist in the short term.
# This is because the workflows that run on the release-trigger branch are from the trigger branch, whereas other files will be from the release branch
# Releases from non-main will still need to use this script, so the workflow cannot be updated to no longer call it
# But for main releases, we don't need it, so we leave it as a no-op script until other branches are either unsupported or updated.

>&2 echo "ci/get-dpm.sh called as no-op, removed in this release line."
