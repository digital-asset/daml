#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR"

execution_log_postfix=${1:-}${2:-}
test_mode=${3:-main}

export LC_ALL=en_US.UTF-8

ARTIFACT_DIRS="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"
mkdir -p "${ARTIFACT_DIRS}/logs"

has_run_all_tests_trailer() {
  if (( 2 == $(git show -s --format=%p HEAD | wc -w) )); then
    ref="HEAD^2"
  else
    ref="HEAD"
  fi
  commit=$(git rev-parse $ref)
  run_all_tests=$(git log -n1 --format="%(trailers:key=run-all-tests,valueonly)" $commit)
  [[ $run_all_tests == "true" ]]
}

has_regenerate_stackage_trailer() {
  if (( 2 == $(git show -s --format=%p HEAD | wc -w) )); then
    ref="HEAD^2"
  else
    ref="HEAD"
  fi
  commit=$(git rev-parse $ref)
  regenerate_stackage=$(git log -n1 --format="%(trailers:key=regenerate-stackage,valueonly)" $commit)
  [[ $regenerate_stackage == "true" ]]
}

tag_filter=""
case $test_mode in
  main)
    echo "running all tests because test mode is 'main'"
    ;;
  # When running against a PR, exclude "main-only" tests, unless the commit message features a
  # 'run-all-tests: true' trailer
  pr)
    if has_run_all_tests_trailer; then
      echo "ignoring 'pr' test mode because the commit message features 'run-all-tests: true'"
    else
      echo "running fewer tests because test mode is 'pr'"
      tag_filter="-main-only"
    fi
    ;;
  *)
    echo "unknown test mode: $test_mode"
    exit 1
    ;;
esac

if [[ "$(uname)" == "Darwin" ]]; then
  tag_filter="$tag_filter,-dont-run-on-darwin,-scaladoc,-pdfdocs"
fi

SKIP_DEV_CANTON_TESTS=false
if [ "$SKIP_DEV_CANTON_TESTS" = "true" ]; then
  tag_filter="$tag_filter,-dev-canton-test"
fi

# remove possible leading comma
tag_filter="${tag_filter#,}"

# Occasionally we end up with a stale sandbox process for a hardcoded
# port number. Not quite sure how we end up with a stale process
# but it happens sufficiently rarely that just killing it here is
# a cheaper solution than having to reset the node.
# Note that lsof returns a non-zero exit code if there is no match.
SANDBOX_PID="$(lsof -ti tcp:6865 || true)"
if [ -n "$SANDBOX_PID" ]; then
    echo $SANDBOX_PID | xargs kill
fi

if [ "${1:-}" = "_m1" ]; then
    bazel="arch -arm64 bazel"
else
    bazel=bazel
fi

if has_regenerate_stackage_trailer; then
  echo "Running @stackage-unpinned//:pin due to 'regenerate-stackage' trailer"
  $bazel run @stackage-unpinned//:pin
  cat stackage_snapshot.json
  exit 1
fi

# Bazel test only builds targets that are dependencies of a test suite so do a full build first.
run_build() {
  bazel build "$@" //... \
    --build_tag_filters "${tag_filter}" \
    --profile build-profile.json \
    --experimental_profile_include_target_label \
    --build_event_json_file build-events.json \
    --build_event_publish_all_actions \
    --execution_log_json_file "$ARTIFACT_DIRS/logs/build_execution${execution_log_postfix}.json.gz"
}

if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" = "arm64" ]; then
  echo "Running on Apple Silicon (arm64). Building with --keep-going and retry on fail."

  max_tries=10
  build_succeeded=false

  for i in $(seq 1 $max_tries); do
    echo "Build attempt ${i}/${max_tries}..."
    if run_build -k; then
      build_succeeded=true
      break
    fi
  done

  if [ "$build_succeeded" = false ]; then
    echo "Build failed after ${max_tries} attempts. Exiting."
    exit 1
  fi

else
  echo "Running on linux or non-arm Apple. Building without retry logic."
  run_build
fi

# Set up a shared PostgreSQL instance.
export POSTGRESQL_ROOT_DIR="${POSTGRESQL_TMP_ROOT_DIR:-$DIR/.tmp-pg}/daml/postgresql"
export POSTGRESQL_DATA_DIR="${POSTGRESQL_ROOT_DIR}/data"
export POSTGRESQL_LOG_FILE="${POSTGRESQL_ROOT_DIR}/postgresql.log"
export POSTGRESQL_HOST='localhost'
export POSTGRESQL_PORT=54321
export POSTGRESQL_USERNAME='test'
export POSTGRESQL_PASSWORD=''
function start_postgresql() {
  mkdir -p "$POSTGRESQL_DATA_DIR"
  bazel run -- @postgresql_dev_env//:initdb --auth=trust --encoding=UNICODE --locale=en_US.UTF-8 --username="$POSTGRESQL_USERNAME" "$POSTGRESQL_DATA_DIR"
  eval "echo \"$(cat ci/postgresql.conf)\"" > "$POSTGRESQL_DATA_DIR/postgresql.conf"
  bazel run -- @postgresql_dev_env//:pg_ctl -w --pgdata="$POSTGRESQL_DATA_DIR" --log="$POSTGRESQL_LOG_FILE" start || {
    if [[ -f "$POSTGRESQL_LOG_FILE" ]]; then
      echo >&2 'PostgreSQL logs:'
      cat >&2 "$POSTGRESQL_LOG_FILE"
    fi
    return 1
  }
}
function stop_postgresql() {
  if [[ -e "$POSTGRESQL_DATA_DIR" ]]; then
    bazel run -- @postgresql_dev_env//:pg_ctl -w --pgdata="$POSTGRESQL_DATA_DIR" --mode=immediate stop || :
    rm -rf "$POSTGRESQL_ROOT_DIR"
  fi
}
trap stop_postgresql EXIT
stop_postgresql # in case it's running from a previous build
start_postgresql

# Run the tests.

echo "Running bazel test with the following tag filters: ${tag_filter}"

$bazel test //... \
  --build_tag_filters "${tag_filter}" \
  --test_tag_filters "${tag_filter}" \
  --test_env "POSTGRESQL_HOST=${POSTGRESQL_HOST}" \
  --test_env "POSTGRESQL_PORT=${POSTGRESQL_PORT}" \
  --test_env "POSTGRESQL_USERNAME=${POSTGRESQL_USERNAME}" \
  --test_env "POSTGRESQL_PASSWORD=${POSTGRESQL_PASSWORD}" \
  --profile test-profile.json \
  --experimental_profile_include_target_label \
  --build_event_json_file test-events.json \
  --build_event_publish_all_actions \
  --execution_log_json_file "$ARTIFACT_DIRS/logs/test_execution${execution_log_postfix}.json.gz"

# Make sure that Bazel query works.
$bazel query 'deps(//...)' >/dev/null

# Check that we can load damlc in ghci
# Disabled on darwin since it sometimes seem to hang and this only
# tests our dev setup rather than our code so issues are not critical.
# FIXME For now this is disabled as we are facing a memory allocation issue:
# ghc-iserv: mmap 131072 bytes at (nil): Cannot allocate memory
# ghc-iserv: Try specifying an address with +RTS -xm<addr> -RTS
# ghc-iserv: internal error: m32_allocator_init: Failed to map
# Note, this should be fixed as of GHC >=9.2.8, see
# https://discourse.haskell.org/t/ghc-9-2-8-is-now-available/6328
#
# if [[ "$(uname)" != "Darwin" ]]; then
#   da-ghci --data yes //compiler/damlc:damlc -e ':run Main.main --help'
# fi

# Test that hls at least builds, we don’t run it since it
# adds 2-5 minutes to each CI run with relatively little benefit. If
# you want to test it manually on upgrades, run
# da-hls compiler/damlc/exe/Main.hs.
da-hls --help
