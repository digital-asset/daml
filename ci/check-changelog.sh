#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

contains_changelog () {
    local awk_script="
        BEGIN { flag = 0 }

        toupper(\$0) ~ /CHANGELOG_BEGIN/ && flag == 0 { flag = 1 }

        toupper(\$0) ~ /CHANGELOG_END/ && flag == 1 { flag = 2 }

        END { print flag }
    "
    [[ 2 == $(git show -s --format=%B $1 | awk "$awk_script") ]]
}

for sha in $(git rev-list ${1:-origin/master}..); do
    if contains_changelog $sha; then
        echo "Commit $sha contains a changelog entry."
        exit 0
    fi
done
echo "
No changelog entry found; please add one. If your PR does not need a
changelog entry, please add an explicit, empty one, i.e. add

CHANGELOG_BEGIN
CHANGELOG_END

to your commit message.
"
exit 1
