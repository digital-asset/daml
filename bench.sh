#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

wi=5
i=25

whatBranch() {
    git rev-parse --abbrev-ref HEAD
}

bench() {
    git checkout $1
    bazel run //daml-lf/scenario-interpreter:scenario-perf -- -f 1 -i $i -wi $wi
}

run() {
    bench $1 2>&1 | grep 'Average]' | xargs echo $1
}

here=$(whatBranch)
base=main

run $here
run $here

run $base
run $base

run $here
run $here

run $base
run $base
