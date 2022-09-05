#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

contains_changelog () {
    local awk_script="
        BEGIN { flag = 0 }

        toupper(\$0) ~ /CHANGELOG_BEGIN/ && flag == 0 { flag = 1 }

        toupper(\$0) ~ /CHANGELOG_END/ && flag == 1 { flag = 2 }

        END { print flag }
    "
    [[ 2 == $(git show -s --format=%b $1 | awk "$awk_script") ]]
}

is_dependabot_pr() {
    local key gpg_dir
    if [ "1" = "$(git rev-list $1.. | wc -l)" ] && [ "dependabot[bot]" = "$(git show -s --format=%an)" ]; then
        key=$(mktemp)
        cat > $key <<DEPENDABOT
-----BEGIN PGP PUBLIC KEY BLOCK-----

mQENBFmUaEEBCACzXTDt6ZnyaVtueZASBzgnAmK13q9Urgch+sKYeIhdymjuMQta
x15OklctmrZtqre5kwPUosG3/B2/ikuPYElcHgGPL4uL5Em6S5C/oozfkYzhwRrT
SQzvYjsE4I34To4UdE9KA97wrQjGoz2Bx72WDLyWwctD3DKQtYeHXswXXtXwKfjQ
7Fy4+Bf5IPh76dA8NJ6UtjjLIDlKqdxLW4atHe6xWFaJ+XdLUtsAroZcXBeWDCPa
buXCDscJcLJRKZVc62gOZXXtPfoHqvUPp3nuLA4YjH9bphbrMWMf810Wxz9JTd3v
yWgGqNY0zbBqeZoGv+TuExlRHT8ASGFS9SVDABEBAAG0NUdpdEh1YiAod2ViLWZs
b3cgY29tbWl0IHNpZ25pbmcpIDxub3JlcGx5QGdpdGh1Yi5jb20+iQEiBBMBCAAW
BQJZlGhBCRBK7hj4Ov3rIwIbAwIZAQAAmQEH/iATWFmi2oxlBh3wAsySNCNV4IPf
DDMeh6j80WT7cgoX7V7xqJOxrfrqPEthQ3hgHIm7b5MPQlUr2q+UPL22t/I+ESF6
9b0QWLFSMJbMSk+BXkvSjH9q8jAO0986/pShPV5DU2sMxnx4LfLfHNhTzjXKokws
+8ptJ8uhMNIDXfXuzkZHIxoXk3rNcjDN5c5X+sK8UBRH092BIJWCOfaQt7v7wig5
4Ra28pM9GbHKXVNxmdLpCFyzvyMuCmINYYADsC848QQFFwnd4EQnupo6QvhEVx1O
j7wDwvuH5dCrLuLwtwXaQh0onG4583p0LGms2Mf5F+Ick6o/4peOlBoZz48=
=Bvzs
-----END PGP PUBLIC KEY BLOCK-----
DEPENDABOT
        gpg_dir=$(mktemp -d)
        GNUPGHOME=$gpg_dir gpg --no-tty --quiet --import $key
        GNUPGHOME=$gpg_dir git verify-commit HEAD
        return $?
    else
        return 1
    fi
}

has_a_changelog() {
    for sha in $(git rev-list $1..); do
        if contains_changelog $sha; then
            echo "Commit $sha contains a changelog entry."
            return 0
        fi
    done
    echo "
No changelog entry found; please add one. Note that the
changelog entry must be in the commit message body excluding the subject in the first line.
If your PR does not need a
changelog entry, please add an explicit, empty one, i.e. add

CHANGELOG_BEGIN
CHANGELOG_END

to your commit message, making sure there's an empty line between the subject and the body.
"
    return 1
}

BASE=${1:-origin/main}

has_a_changelog $BASE || is_dependabot_pr $BASE
