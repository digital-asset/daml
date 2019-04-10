#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euxo pipefail

eval "$($(dirname "$0")/dev-env/bin/dade-assist)"

execution_log_postfix=${1:-}

export LC_ALL=en_US.UTF-8

# check for scala code style
scalafmt_ret=0
./scalafmt.sh --test --quiet || scalafmt_ret=$?
if [[ scalafmt_ret -ne 0 ]]; then
  echo "scalafmt returned ${scalafmt_ret}. Please run ./scalafmt.sh to fix the formatting of your code."
  exit $scalafmt_ret
fi

# Check for correct copyrights
dade-copyright-headers check .

EXEC_LOG_DIR="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"

# Bazel test only builds targets that are dependencies of a test suite
# so do a full build first.
(
  cd compiler
  # Bazel also limits cache downloads by -j so increasing this to a ridiculous value
  # helps. Bazel separately controls the number of jobs using CPUs so this should not
  # overload machines.
  bazel build -j 200 //... --experimental_execution_log_file "$EXEC_LOG_DIR/build_execution${execution_log_postfix}.log"
)
bazel test -j 200 //... --experimental_execution_log_file "$EXEC_LOG_DIR/test_execution${execution_log_postfix}.log"
# Make sure that Bazel query works.
bazel query 'deps(//...)' > /dev/null
# Execute Sandbox performance tests if on master
# On Jenkins we never run them as BUILD_SOURCEBRANCHNAME isn’t set.
if [[ "${BUILD_SOURCEBRANCHNAME:-master}" = master ]]; then
    bazel run -- //ledger/sandbox-perf -i1 -f1 -wi 1 -bm avgt -rf json -rff sandbox-perf.json # 1 warmup, 1 iterations in 1 fork
fi

# Check that we can load damlc in ghci
da-ghci damlc -e '()'

# We have a Bazel test that is meant to run HLint, but we're a little sceptical of it
# If we get this far, but hlint fails, that's a problem we should fix
function bad_hlint() {
  echo "UNEXPECTED HLINT FAILURE: The Bazel rules should have spotted this, please raise a GitHub issue"
}
trap bad_hlint EXIT
for dir in daml-foundations da-assistant daml-assistant libs-haskell compiler; do
    pushd $dir
    hlint --git -j4
    popd
done
trap - EXIT
