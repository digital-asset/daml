#!/usr/bin/env bats
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Remember where the hook is
BASE_DIR=$(dirname $BATS_TEST_DIRNAME)
# Set up directories for our git repos
REPO_ORIGIN_DIRECTORY=$(mktemp -d)
REPO_DIRECTORY=$(mktemp -d)

setup_repo() {
    repo=$1
    # Set up a git repo
    cd $repo
    git init $2
    git config user.email "test@git-confirm"
    git config user.name "Git Confirm Tests"
}

goto_origin() {
    cd "$REPO_ORIGIN_DIRECTORY"
}

goto_repo() {
    cd "$REPO_DIRECTORY"
}

setup() {
    setup_repo "$REPO_ORIGIN_DIRECTORY" --bare

    setup_repo "$REPO_DIRECTORY"
    goto_repo
    git remote add origin "$REPO_ORIGIN_DIRECTORY"
    # git fetch --all
    # git checkout -t origin/master

    echo "fake fake fake" >some_file
    git add some_file
    git commit -m "Initial commit"
    # this one will work
    git push -f -u origin master

    # install the hook
    cp "$BATS_TEST_DIRNAME/../../git-hooks/pre-push" .git/hooks/
}

teardown() {
    if [ $BATS_TEST_COMPLETED ]; then
        echo "Deleting temporary directories"
        rm -rf "$REPO_DIRECTORY"
        rm -rf "$REPO_ORIGIN_DIRECTORY"
    else
        echo "** Did not delete '$REPO_ORIGIN_DIRECTORY' and '$REPO_DIRECTORY', as test failed **"
    fi

    cd $BATS_TEST_DIRNAME
}

@test "Should allow regular push to a newly created branch" {
    goto_repo
    git checkout -b new_branch

    echo "Some content" > my_file
    git add my_file
    git commit -m "Content"

    run git push --set-upstream origin new_branch

    [ "$status" -eq 0 ]
}

@test "Should allow regular force push to a newly created branch" {
    goto_repo
    git checkout -b new_branch

    echo "Some content" > my_file
    git add my_file
    git commit -m "Content"

    run git push --set-upstream -f origin new_branch
    [ "$status" -eq 0 ]

    run git push --set-upstream -f origin HEAD
    [ "$status" -eq 0 ]

}

@test "Should prevent force push to master" {
    goto_repo
    git checkout -b new_branch

    echo "Some content" > my_file
    git add my_file
    git commit -m "Content"

    run git push -f origin HEAD:master
    [ "$status" -ne 0 ]

    run git push -f origin HEAD HEAD:master
    [ "$status" -ne 0 ]
}

@test "Should prevent force push to master by full ref" {
    goto_repo
    git checkout -b new_branch

    echo "Some content" > my_file
    git add my_file
    git commit -m "Content"

    run git push -f origin HEAD:refs/heads/master
    [ "$status" -ne 0 ]
}

@test "Should not prevent force push to master when --no-verify is on" {
    goto_repo
    git checkout -b new_branch

    echo "Some content" > my_file
    git add my_file
    git commit -m "Content"

    run git push -f --no-verify origin master
    [ "$status" -eq 0 ]
}

@test "Should prevent deleting master branch via '-d'" {
    # this should work by default, as git prevents deleting current branch
    run git push -d origin master
    [ "$status" -ne 0 ]
}

@test "Should prevent deleting master branch via ':master'" {
    # this should work by default, as git prevents deleting current branch
    run git push origin :master
    [ "$status" -ne 0 ]
}

@test "Should prevent force push when multiple protected branches" {
    goto_repo
    sed -i.bak "s/'master'/'master' 'another'/g" .git/hooks/pre-push

    run git push -f origin :another
    [ "$status" -ne 0 ]
}
