#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

echo "TODO: ci/get-dpm.sh is a no-op kept only because release-trigger calls it; ci/publish-oci.sh runs dpm through 'bazelisk run @dpm_binary//:dpm'. Remove it once release-trigger no longer calls it" >&2
