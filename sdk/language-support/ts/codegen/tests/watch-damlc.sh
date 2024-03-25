#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DAML=$DIR/daml

pushd $DAML
bazel run //:damlc -- build --project-root $DAML
fswatch -0 --exclude ".*" --include "\\.daml$" $DAML | xargs -0 -I {} bazel run //:damlc -- build --project-root $DAML
