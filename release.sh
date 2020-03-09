#!/usr/bin/env bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

uhoh() {
    echo "
    It looks like this script failed to complete. Please check the status
    of the LATEST file and consider running this script again."
}

trap uhoh EXIT

CURRENT=$(cat LATEST | awk '{print $2}')
STABLE_REGEX="\d+\.\d+\.\d+"
VERSION_REGEX="^${STABLE_REGEX}(-snapshot\.\d{8}\.\d+\.[0-9a-f]{8})?$"

check() {
    if ! echo $CURRENT | grep -q -P $VERSION_REGEX; then
        echo "Invalid version number in LATEST file, needs manual correction."
        exit 1
    else
        echo -n "Valid version number ("
        if is_stable $CURRENT; then
            echo -n "stable"
        else
            echo -n "snapshot"
        fi
        echo ")."
    fi
}


is_stable() {
    local version="$1"
    echo "$version" | grep -q -P "^${STABLE_REGEX}$"
}

make_snapshot() {
    local sha=$1
    local commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d $sha)
    local number_of_commits=$(git rev-list --count $sha)
    local commit_sha_8=$(git log -n1 --format=%h --abbrev=8 $sha)
    local prerelease="snapshot.$commit_date.$number_of_commits.$commit_sha_8"
    if is_stable "$CURRENT"; then
        local stable="$CURRENT"
    else
        local stable=$(echo "$CURRENT" | grep -o -P "^$STABLE_REGEX")
    fi
    echo "$sha $stable-$prerelease" > LATEST
    echo "Updated LATEST file."
}

display_help() {
    cat <<EOF
This script is meant to help with managing the LATEST file. Usage:

$0 snapshot SHA
        Updates the LATEST file to point to the given SHA (which must be a
        valid git reference to a commit on origin/master). If the current
        version defined in LATEST is already a snapshot, keeps the stable part
        of the version unchanged; otherwise, increments the patch number.

Any other invocation will display this help message.

Note: at the moment, changing the version string for a stable release is left
as a manual exercice, but that may change in the future.
EOF
}

case $1 in
    snapshot)
        check
        git fetch origin master 1>/dev/null 2>&1
        if [ -n "${2+x}" ] && git merge-base --is-ancestor $2 origin/master >/dev/null; then
            make_snapshot $(git rev-parse $2)
        else
            display_help
        fi
        ;;
    check)
        check
        ;;
    *)
        display_help
        ;;
esac

trap - EXIT
