#!/usr/bin/env bats
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


@test "Path with strange characters" {
    cd "$(mktemp -d)"

    mkdir 'strange { name'
    pushd 'strange { name'
    pipenv lock
    printf '#!/usr/bin/env runpipenv\nimport sys; print(sys.argv)' > echo.py
    chmod +x echo.py
    popd

    run 'strange { name/echo.py'
    [ "$status" -eq 0 ]
}

@test "Symlinked script" {
    cd "$(mktemp -d)"

    pipenv lock
    printf '#!/usr/bin/env runpipenv\nimport sys; print(sys.argv)' > echo.py
    chmod +x echo.py
    ln -s echo.py my-echo

    run ./my-echo
    [ "$status" -eq 0 ]
}

@test "Arguments with spaces" {
    cd "$(mktemp -d)"

    pipenv lock
    printf '#!/usr/bin/env runpipenv\nimport sys; print(sys.argv)' > echo.py
    chmod +x echo.py

    run ./echo.py first-arg 'second arg'
    [ "$status" -eq 0 ]
    echo
    echo "actual:   ${lines[-1]}"
    echo "expected: ['echo.py', 'first-arg', 'second arg']"
    echo
    [ "${lines[-1]}" = "['./echo.py', 'first-arg', 'second arg']" ]
}
