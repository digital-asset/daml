#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

uhoh() {
    echo "
    It looks like this script failed to complete. Please check the status
    of the LATEST file and consider running this script again."
}

trap uhoh EXIT

STABLE_REGEX="\d+\.\d+\.\d+"
VERSION_REGEX="^${STABLE_REGEX}(-snapshot\.\d{8}\.\d+(\.\d+)?\.[0-9a-f]{8})?$"

function file_ends_with_newline() {
    [[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

check() {
    local sha ver ver_sha
    if ! file_ends_with_newline LATEST; then
        echo "LATEST file does not end with newline. Please correct."
        exit 1
    fi
    while read line; do
        sha=$(echo "$line" | gawk '{print $1}')
        ver=$(echo "$line" | gawk '{print $2}')
        if ! echo "$ver" | grep -q -P $VERSION_REGEX; then
            echo "Invalid version number in LATEST file, needs manual correction."
            echo "Offending version: '$ver'."
            exit 1
        fi
        if ! is_stable $ver; then
            ver_sha=$(echo $ver | sed 's/.*\.//')
            if ! [ "${sha:0:8}" = "$ver_sha" ]; then
                echo "$ver does not match $sha, please correct. ($ver_sha != ${sha:0:8})"
                exit 1
            fi
        fi
    done < LATEST
}

is_stable() {
    local version="$1"
    echo "$version" | grep -q -P "^${STABLE_REGEX}$"
}

make_snapshot() {
    local sha prefix commit_date number_of_commits commit_sha_8
    sha=$1
    prefix=$2
    commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d $sha)
    number_of_commits=$(git rev-list --count $sha)
    commit_sha_8=$(git log -n1 --format=%h --abbrev=8 $sha)
    echo "$1 $2-snapshot.$commit_date.$number_of_commits.0.$commit_sha_8"
}

display_help() {
    cat <<EOF
This script is meant to help with managing releases. Usage:

$0 snapshot SHA PREFIX
        Prints the snapshot line for commit SHA as a release candidate for
        version PREFIX. For example:

        $ $0 snapshot cc880e2 0.1.2
        cc880e290b2311d0bf05d58c7d75c50784c0131c 0.1.2-snapshot.20200513.4174.0.cc880e29

        Any non-ambiguous git commit reference can be given as SHA.

$0 check
        Checks that each line of the LATEST file is well-formed.

Any other invocation will display this help message.
EOF
}

if [ -z "${1+x}" ]; then
    display_help
    exit 1
fi

commit_belongs_to_release_branch() {
    local sha branches
    sha=$1
    branches="$(git branch --contains $sha --format='%(refname:short)')"
    for branch in $branches; do
        if [ "$branch" = "master" ] || [ "${branch:0:8}" = "release/" ]; then
            return 0
        fi
    done
    return 1
}

case $1 in
    snapshot)
        if [ -n "${2+x}" ] && [ -n "${3+x}" ]; then
            if ! commit_belongs_to_release_branch $2; then
                echo "WARNING: Commit does not belong to a release branch."
            fi
            make_snapshot $(git rev-parse $2) $3
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
