#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

sphinx-build -M html $DIR/../docs/sharable $DIR/../docs/build -c $DIR/../docs/sphinx-config -E -W

if [[ " $* " == *" --with-preview "* ]]; then
    python -m http.server -d $DIR/../docs/build/html
fi
