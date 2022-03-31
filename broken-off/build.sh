#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [ -n "${WINDOW:-}" ]; then
    # `dirname` doesn't seem to be available on Windows
    echo "WARNING: Running on Windows, guessing folders. Please run from script dir."
else
    cd "$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
fi

for d in art/scalatest-utils; do
    (
        version=$(./version.sh $d)
        echo "Building $d, version $version"
        cd $d
        ./build.sh $version ../../../build-inputs
    )
done
