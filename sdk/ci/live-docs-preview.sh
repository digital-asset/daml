#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

watchexec --restart --watch $DIR/../docs/manually-written/ -- "$DIR/../ci/synchronize-docs.sh && $DIR/../ci/docs-sphinx-build.sh --with-preview"
