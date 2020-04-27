#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
VERSION_REGEX="^${STABLE_REGEX}(-snapshot\.\d{8}\.\d+(\.\d+)?\.[0-9a-f]{8})?$"

release_sha() {
    git show $1:LATEST | gawk '{print $1}'
}

release_version() {
    git show $1:LATEST | gawk '{print $2}'
}

check() {
    if ! echo $(release_version HEAD) | grep -q -P $VERSION_REGEX; then
        echo "Invalid version number in LATEST file, needs manual correction."
        exit 1
    else
        echo -n "Valid version number ("
        if is_stable $(release_version HEAD); then
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
    local prerelease="snapshot.$commit_date.$number_of_commits.0.$commit_sha_8"
    if is_stable "$(release_version HEAD)"; then
        local stable="$(release_version HEAD)"
    else
        local stable=$(echo "$(release_version HEAD)" | grep -o -P "^$STABLE_REGEX")
    fi
    echo "$sha $stable-$prerelease" > LATEST
    echo "Updated LATEST file."
}

parse_range() {
    case $1 in
        head)
            git rev-parse HEAD
        ;;
        latest)
            release_sha HEAD
        ;;
        previous)
            release_sha $(git log -n2 --format=%H LATEST | sed 1d)
        ;;
        stable)
            for sha in $(git log --format=%H LATEST | sed 1d); do
                if is_stable $(release_version $sha); then
                    release_sha $sha
                    break
                fi
            done
        ;;
        *)
            display_help
            exit 1
        ;;
    esac
}

display_help() {
    cat <<EOF
This script is meant to help with managing releases. Usage:

$0 snapshot SHA
        Updates the LATEST file to point to the given SHA (which must be a
        valid git reference to a commit on origin/master). If the current
        version defined in LATEST is already a snapshot, keeps the stable part
        of the version unchanged; otherwise, increments the patch number.

$0 check
        Checks that the LATEST file is well-formed and prints a message saying
        whether the latest release is considered stable or snapshot.

$0 changes <start> <end>
        Prints the changes between start and end. In this context, possible
        values are, in order:
          head
              The current commit.
          latest
              The commit pointed at by the LATEST file in the current commit.
          previous
              The most recent release (stable or snapshot) before the current
              one.
          stable
              The most recent stable release before the current one.
        Specifying them out of order is not supported.

Any other invocation will display this help message.

Note: at the moment, changing the version string for a stable release is left
as a manual exercice, but that may change in the future.
EOF
}

if [ -z "${1+x}" ]; then
    display_help
    exit 1
fi

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
    changes)
        if [ -z "${2+x}" ] || [ -z "${3+x}" ]; then
            display_help
            exit 1
        else
            ./unreleased.sh $(parse_range $2)..$(parse_range $3)
        fi
    ;;
    *)
        display_help
    ;;
esac

trap - EXIT
