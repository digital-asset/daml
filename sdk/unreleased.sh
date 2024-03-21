#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if [ "$#" -ne 1 ]; then
    echo >&2 "Usage: ./unreleased.sh <revision range>"
    echo >&2 "Prints all changelog entries added by the given revision range"
    echo >&2 "For info about <revision range> please see gitrevisions(7)"
    exit 64
fi

set -euo pipefail

COMMITS_IN_GIVEN_RANGE=$(git log --format=%H "$1")

extract_changelog () {
    awk '
        # Skip empty lines.
        /^[[:space:]]*$/ {next}

        # Take entire line, uppercase, compare to CHANGELOG_END.
        # If it matches, set flag to 0 (false) and skip current line.
        toupper($0) ~ /CHANGELOG_END/ { flag=0; next }

        # If uppercased line matches CHANGELOG_BEGIN, skip current line and set
        # flag to 1 (true).
        toupper($0) ~ /CHANGELOG_BEGIN/ { flag=1; next }

        # Because all previous cases skip the current line, if we reach this
        # point we know that the current line is not blank, does not contain
        # CHANGELOG_END, and does not contain CHANGELOG_BEGIN. Here we match
        # the line based on the value of flag, regardless of the content of the
        # line. Because there is no action associated with this condition, the
        # default one is applied when it matches (i.e. when flag != 0), which
        # is to print the entire current line.
        flag
    '
}

for SHA in $COMMITS_IN_GIVEN_RANGE; do
    COMMIT_MESSAGE_BODY=$(git show --quiet --format=%b "$SHA")
    COMMIT_CHANGELOG=$(echo "$COMMIT_MESSAGE_BODY" | extract_changelog)
    if [[ ! -z "$COMMIT_CHANGELOG" ]]; then
        COMMIT_AUTHOR_AND_SUBJECT=$(git show --quiet --format="* %s (committer: %an | hash: %h)" "$SHA")
        echo "$COMMIT_AUTHOR_AND_SUBJECT"
        echo "$COMMIT_CHANGELOG"
        echo "----------------"
    fi
done
