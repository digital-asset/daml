#!/bin/bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DAML=$DIR/daml

pushd $DAML
daml init
daml build
fswatch -0 --exclude ".*" --include "\\.daml$" $DAML | xargs -0 -I {} daml build
