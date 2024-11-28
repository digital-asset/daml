#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# The usage of this script is documented in /release/RELEASE.md

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

uhoh() {
    echo "
    It looks like this script failed to complete. Please check the status
    of the LATEST file and consider running this script again."
}

trap uhoh EXIT

STABLE_REGEX="\d+\.\d+\.\d+"
SNAPSHOT_REGEX="^${STABLE_REGEX}-snapshot\.\d{8}\.\d+(\.\d+)?\.v[0-9a-f]{8}$"
ADHOC_REGEX="^${STABLE_REGEX}-adhoc\.\d{8}\.\d+(\.\d+)?\.v[0-9a-f]{8}$"
RC_REGEX="^${STABLE_REGEX}-rc\d+$"
VERSION_REGEX="(^$STABLE_REGEX$)|($SNAPSHOT_REGEX)|($ADHOC_REGEX)|($RC_REGEX)"

function file_ends_with_newline() {
    [[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

commit_belongs_to_branch() {
    git branch --all --format='%(refname:short)' --contains="$1" \
      | grep -F -x "$2" >/dev/null 2>/dev/null # grep -q flag quits early and causes a SIGPIPE signal in git branch
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
        split=$(echo "$line" | gawk '{print $3}')
        if ! echo "$ver" | grep -q -P $VERSION_REGEX; then
            echo "Invalid version number in LATEST file, needs manual correction."
            echo "Offending version: '$ver'."
            exit 1
        fi
        if is_snapshot_or_adhoc $ver; then
            ver_sha=$(echo $ver | sed 's/.*\.v//')
            if ! [ "${sha:0:8}" = "$ver_sha" ]; then
                echo "$ver does not match $sha, please correct. ($ver_sha != ${sha:0:8})"
                exit 1
            fi
        fi

        version_without_trailer=$(echo "$ver" | grep -oP "^$STABLE_REGEX")
        major_version=$(echo "$version_without_trailer" | sed -E "s/^([0-9]+)\.([0-9]+)\.([0-9]+)$/\1/")
        minor_version=$(echo "$version_without_trailer" | sed -E "s/^([0-9]+)\.([0-9]+)\.([0-9]+)$/\2/")
        if is_stable_or_rc $ver; then
            target_branch="origin/release/$major_version.$minor_version.x"
            if missing_target_branch "$version_without_trailer"; then
                echo "Skipping check for release '$line' because version $version_without_trailer has a target branch '$target_branch' which does not exist anymore."
            elif ! commit_belongs_to_branch "$sha" "$target_branch"; then
                echo "Error while checking '$line' is a valid release:"
                echo "Stable or RC release with version $version_without_trailer must come from branch '$target_branch', but commit $sha does not belong to that branch."
                echo "Stable releases or release candidates must come from their release branch."
                echo "Commit $sha belongs to the following release branches:"
                find_release_branches_for_commit $sha
                exit 1
            else
                echo "Commit $sha belongs to branch '$target_branch', release '$line' is valid"
            fi
        elif is_snapshot $ver; then
            target_branch=$(release_branch_for_version $version_without_trailer)
            if ! commit_belongs_to_branch "$sha" "$target_branch"; then
                echo "Error while checking '$line' is a valid release:"
                echo "Snapshot release with version $version_without_trailer must come from branch '$target_branch', but commit $sha does not belong to that branch."
                echo "Commit $sha belongs to the following release branches:"
                find_release_branches_for_commit $sha
                exit 1
            else
                echo "Commit $sha belongs to branch '$target_branch', release '$line' is valid"
            fi
        elif is_adhoc $ver; then
            echo "Skipping check for release '$line' because version $ver is an adhoc version."
        else
            echo "Error while checking '$line' is a valid release:"
            echo "Version $ver does not match the regex for a snapshot version, nor an adhoc version, nor a stable version, nor a release candidate version."
            exit 1
        fi

        if [ ! -z "$split" ] && [ "$split" != "SPLIT_RELEASE" ]; then
            echo "Invalid entry in third column, must be SPLIT_RELEASE or non-existent."
        fi
    done < LATEST
}

missing_target_branch() {
    local version="$1"
    echo "$version" | grep -x -P -q "1\.(4|5|7|12)\.[0-9]+"
}

is_stable_or_rc() {
    local version="$1"
    echo "$version" | grep -q -P "(^${STABLE_REGEX}|${RC_REGEX})$"
}

is_snapshot_or_adhoc() (
    echo "$1" | grep -q -P "($SNAPSHOT_REGEX|$ADHOC_REGEX)"
)

is_snapshot() (
    echo "$1" | grep -q -P "($SNAPSHOT_REGEX)"
)

is_adhoc() (
    echo "$1" | grep -q -P "($ADHOC_REGEX)"
)

make_snapshot() {
    local sha prefix commit_date number_of_commits commit_sha_8
    t=$1
    sha=$2
    prefix=$3
    commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d $sha)
    number_of_commits=$(git rev-list --count $sha)
    commit_sha_8=$(git log -n1 --format=%h --abbrev=8 $sha)
    echo "$sha $prefix-$t.$commit_date.$number_of_commits.0.v$commit_sha_8 SPLIT_RELEASE"
}

display_help() {
    cat <<EOF
This script is meant to help with managing releases. Usage:

$0 snapshot SHA PREFIX
        Prints the snapshot line for commit SHA as a release candidate for
        version PREFIX. For example:

        $ $0 snapshot cc880e2 0.1.2
        cc880e290b2311d0bf05d58c7d75c50784c0131c 0.1.2-snapshot.20200513.4174.0.cc880e29 SPLIT_RELEASE

        Any non-ambiguous git commit reference can be given as SHA.

$0 check
        Checks that each line of the LATEST file is well-formed.

Any other invocation will display this help message.

For further details, see the documentation in /release/RELEASE.md
EOF
}

if [ -z "${1+x}" ]; then
    display_help
    exit 1
fi

available_release_lines() {
    git branch --all --format='%(refname:short)' \
      | grep -E "^origin/release/$1\.[0-9]+\.x$" \
      | jq -sR 'split("\n") | map(select(length > 0)) | map(split("/")[2] | split(".")[0:2] | map(tonumber))'
}

release_branch_for_version() {
    version="$1"
    target_major_version=$(echo "$version" | sed -E "s/^([0-9]+)\.([0-9]+)\.([0-9]+)$/\1/")
    target_minor_version=$(echo "$version" | sed -E "s/^([0-9]+)\.([0-9]+)\.([0-9]+)$/\2/")
    available_release_lines "$target_major_version" | jq -r """
      max |
      if .[1] < $target_minor_version then
          if $target_major_version == 2 then
              \"origin/main-2.x\"
          else
              \"origin/main\"
          end
      else
          \"origin/release/$target_major_version.$target_minor_version.x\"
      end
    """
}

find_release_branches_for_commit() {
    git branch --all --format='%(refname:short)' --contains="$1" \
      | grep ${2:-} -E -x "origin/(release/[0-9]+.[0-9]+.x|main|main-2.x)"
}

check_new_version_and_commit() {
    version=$1
    commit=$2
    target_branch=$(release_branch_for_version $version)
    if commit_belongs_to_branch $2 "$target_branch"; then
      echo exact
    else
      if find_release_branches_for_commit $2 -q; then
        echo failure
      else
        echo adhoc
      fi
    fi
}

case $1 in
    snapshot)
        if [ -n "${2+x}" ] && [ -n "${3+x}" ]; then
            if ! echo "$3" | grep -q -E '[0-9]+\.[0-9]+\.[0-9]+'; then
                echo "Supplied version '$3' is not a valid version. Versions must be three integers separated by dots, e.g. 2.9.7 or 3.0.4" >&2
                exit 1
            fi
            target_branch=$(release_branch_for_version $3)
            case $(check_new_version_and_commit $3 $2) in
              exact)
                make_snapshot snapshot $(git rev-parse $2) $3
                ;;
              adhoc)
                echo "WARNING: The expected release branch for version $3 is '$target_branch', but commit $2 is not on any release branch. Generating an adhoc release name..." >&2
                make_snapshot adhoc $(git rev-parse $2) $3
                ;;
              failure)
                echo "ERROR: The expected release branch for version $3 is '$target_branch', but commit $2 belongs to a different release branch: $(find_release_branches_for_commit $2 | tr '\n' ' ')" >&2
                exit 1
                ;;
            esac
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
