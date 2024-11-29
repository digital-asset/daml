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

check_ver() {
    local version="$1"
    local regex="$2"
    echo "$version" | grep -x -P -q "$regex"
}

is_stable()   { check_ver "$1" "(${STABLE_REGEX})"; }
is_rc()       { check_ver "$1" "(${STABLE_REGEX}-rc\d+)"; }
is_snapshot() { check_ver "$1" "(${STABLE_REGEX}-snapshot\.\d{8}\.\d+(\.\d+)?\.v[0-9a-f]{8})"; }
is_adhoc()    { check_ver "$1" "(${STABLE_REGEX}-adhoc\.\d{8}\.\d+(\.\d+)?\.v[0-9a-f]{8})"; }
is_version()  { is_stable "$ver" || is_rc "$ver" || is_snapshot "$ver" || is_adhoc "$ver"; }

function file_ends_with_newline() {
    [[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

check() {
    local sha ver ver_sha
    if ! file_ends_with_newline LATEST; then
        echo "LATEST file does not end with newline. Please correct."
        exit 1
    fi
    while read -r line; do
        sha=$(echo "$line" | gawk '{print $1}')
        ver=$(echo "$line" | gawk '{print $2}')
        split=$(echo "$line" | gawk '{print $3}')
        if ! is_version "$ver"; then
            echo "Invalid version number in LATEST file, needs manual correction."
            echo "Offending version: '$ver'."
            exit 1
        fi
        if is_snapshot "$ver" || is_adhoc "$ver"; then
            ver_sha=${ver/*.v/}
            if ! [ "${sha:0:8}" = "$ver_sha" ]; then
                echo "$ver does not match $sha, please correct. ($ver_sha != ${sha:0:8})"
                exit 1
            fi
        fi

        version_without_trailer=$(echo "$ver" | grep -oP "^$STABLE_REGEX")
        version_arr=(${version_without_trailer//./ })
        if is_stable "$ver" || is_rc "$ver"; then
            target_branch="origin/release/${version_arr[0]}.${version_arr[1]}.x"
            if missing_target_branch "$version_without_trailer"; then
                echo "Skipping check for release '$line' because version $version_without_trailer has a target branch '$target_branch' which does not exist anymore."
            elif ! commit_belongs_to_branch "$sha" "$target_branch"; then
                echo "Error while checking '$line' is a valid release:"
                echo "Stable or RC release with version $version_without_trailer must come from branch '$target_branch', but commit $sha does not belong to that branch."
                echo "Stable releases or release candidates must come from their release branch."
                echo "Commit $sha belongs to the following release branches:"
                find_release_branches_for_commit "$sha"
                exit 1
            else
                echo "Commit $sha belongs to branch '$target_branch', release '$line' is valid"
            fi
        elif is_snapshot "$ver"; then
            target_branch=$(release_branch_for_version "$version_without_trailer")
            if ! commit_belongs_to_branch "$sha" "$target_branch"; then
                echo "Error while checking '$line' is a valid release:"
                echo "Snapshot release with version $version_without_trailer must come from branch '$target_branch', but commit $sha does not belong to that branch."
                echo "Commit $sha belongs to the following release branches:"
                find_release_branches_for_commit "$sha"
                exit 1
            else
                echo "Commit $sha belongs to branch '$target_branch', release '$line' is valid"
            fi
        elif is_adhoc "$ver"; then
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

make_snapshot() {
    local sha prefix commit_date number_of_commits commit_sha_8
    t=$1
    sha=$2
    prefix=$3
    commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d "$sha")
    number_of_commits=$(git rev-list --count "$sha")
    commit_sha_8=$(git log -n1 --format=%h --abbrev=8 "$sha")
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

# Takes a major version, prints all release lines for that version
available_release_lines() {
    git branch --all --format='%(refname:short)' \
      | grep -E "^origin/release/$1\.[0-9]+\.x$" \
      | jq -sR 'split("\n") | map(select(length > 0)) | map(split("/")[2] | split(".")[0:2] | map(tonumber))'
}

# Takes a version x.y.z as its first argument, prints the expected branch for
# producing those release lines.
release_branch_for_version() {
    version="$1"
    version_arr=(${version//./ })
    available_release_lines "${version_arr[0]}" | jq -r """
      max |
      if .[1] < ${version_arr[1]} then
          if ${version_arr[0]} == 2 then
              \"origin/main-2.x\"
          else
              \"origin/main\"
          end
      else
          \"origin/release/${version_arr[0]}.${version_arr[1]}.x\"
      end
    """
}

# Takes a commit reference as its first argument.
find_release_branches_for_commit() {
    git branch --all --format='%(refname:short)' --contains="$1" \
      | grep "${2:-}" -E -x "origin/(release/[0-9]+.[0-9]+.x|main|main-2.x)"
}

# Takes a commit reference as its first argument, and a branch as its second argument.
commit_belongs_to_branch() {
    commit=$1
    branch=$2
    git branch --all --format='%(refname:short)' --contains="$commit" \
      | grep -F -x "$branch" &>/dev/null # grep -q flag quits early and causes a SIGPIPE signal in git branch
}

# Takes a version x.y.z as its first argument, a commit reference as its second argument
check_new_version_and_commit() {
    version=$1
    commit=$2
    target_branch=$(release_branch_for_version "$version")
    if commit_belongs_to_branch "$commit" "$target_branch"; then
      echo exact
    else
      if find_release_branches_for_commit "$commit" -q; then
        echo failure
      else
        echo adhoc
      fi
    fi
}

case $1 in
    snapshot)
        if [ -n "${2+x}" ] && [ -n "${3+x}" ]; then
            commit=$2
            version=$3
            if ! echo "$version" | grep -q -E '[0-9]+\.[0-9]+\.[0-9]+'; then
                echo "Supplied version '$version' is not a valid version. Versions must be three integers separated by dots, e.g. 2.9.7 or 3.0.4" >&2
                exit 1
            fi
            target_branch=$(release_branch_for_version "$version")
            case $(check_new_version_and_commit "$version" "$commit") in
              exact)
                make_snapshot snapshot "$(git rev-parse "$commit")" "$version"
                ;;
              adhoc)
                echo "WARNING: The expected release branch for version $version is '$target_branch', but commit $commit is not on any release branch. Generating an adhoc release name..." >&2
                make_snapshot adhoc "$(git rev-parse "$commit")" "$version"
                ;;
              failure)
                echo "ERROR: The expected release branch for version $version is '$target_branch', but commit $commit belongs to a different release branch: $(find_release_branches_for_commit "$commit" | tr '\n' ' ')" >&2
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
