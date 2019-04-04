#!/usr/bin/env bats
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


@test "Check that PATH is unchanged when invoking Bash" {
    PATH="/some/directory:$PATH"
    echo '$PATH in inner Bash is:'
    bash -c 'echo "  $PATH" | sed "s/:/\n  /g"'

    [ $(bash -c 'echo "$PATH" | cut -d : -f 1') = "/some/directory" ]
}

@test "Checking that PATH contain dev-env/bin just once" {
    echo '$PATH is:'
    echo "  $PATH" | sed "s/:/\n  /g"

    [ $(echo "$PATH" | tr : '\n' | grep -c dev-env/bin) -eq 1 ]
}

@test "Checking for duplicate entries on PYTHONPATH" {
    echo '$PYTHONPATH is'
    echo "  $PYTHONPATH" | sed "s/:/\n  /g"

    [ "$PYTHONPATH" = "." ]
}
