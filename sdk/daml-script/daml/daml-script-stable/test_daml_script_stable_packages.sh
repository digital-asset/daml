#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

diff $1 $2 || (>&2 echo "Stable packages have been updated without updating the stable-packages.bzl file. Please run \"bazel run //daml-script/daml/daml-script-stable:update-script-stable-packages\" to update it." && exit 1)
