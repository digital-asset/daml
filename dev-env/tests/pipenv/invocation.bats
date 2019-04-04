#!/usr/bin/env bats
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


@test "Pipenv invocation" {
    cd "$(mktemp -d)"

    run pipenv lock
    [ "$status" -eq 0 ]
}
