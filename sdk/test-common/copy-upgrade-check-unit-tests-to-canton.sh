#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$DIR/.."

# Make sure a local canton path is specified before we do anything
LOCAL_CANTON_PATH=$(./canton/get-local-canton-path.sh)
CANTON_DESTINATION=$LOCAL_CANTON_PATH/community/daml-lf/upgrade-check/src/test/damlParallel

# Set up work directory for temporarily staging all our changes
WORKDIR=$(mktemp -d)
echo "Temporary working directory: $WORKDIR"
cp -r test-common/src/main/daml/upgrades/* "$WORKDIR"
echo "packages:" > "$WORKDIR/multi-package.yaml"

# If no test names are specified, glob all upgrade test names
if [[ "$#" -eq 0 ]]; then
  mapfile -t NAMES < <(bazel query '//test-common:*' | grep 'upgrades-' | grep '\.dar$' | sed -E 's,//test-common:upgrades-,,' | sed -E 's/\.dar//')
else
  NAMES=("$@")
fi

# Build the yaml file and the srcout for every project
BAZEL_TARGETS=()
for NAME in "${NAMES[@]}"; do
  BAZEL_TARGETS+=(//test-common:upgrades-$NAME.yaml //test-common:upgrades-$NAME.srcout)
done
bazel build "${BAZEL_TARGETS[@]}"

# For each test package, find its sources' root, and copy the daml.yaml into there
for NAME in "${NAMES[@]}"; do
  echo "Process $NAME..."
  SOURCE_ROOT=$(
    cat bazel-bin/test-common/upgrades-$NAME.srcout |\
      sed '$!{N;s/^\(.*\).*\n\1.*$/\1\n\1/;D;}' |\
      sed -E 's,/[^/]+\.daml$,,' |\
      sed -E 's,/$,,' |\
      sed -E 's,^test-common/src/main/daml/upgrades/,,'
  )

  # Fix paths/filenames in the daml.yaml files
  cat bazel-bin/test-common/upgrades-$NAME.yaml | yq -y '''
    .dependencies |=
      map(if test("daml-script") then "daml-script" else . end)
  | .["data-dependencies"] |=
      (values | map("${TARGET_ROOT}/" + sub(".dar.dar$"; ".dar")))
  | .upgrades |=
      (values | "${TARGET_ROOT}/" + sub(".dar.dar$"; ".dar"))
  | .["build-options"] +=
      (values | ["--output=${TARGET_ROOT}/" + "upgrades-'''$NAME'''.dar"])
  | .["override-components"] =
    {
      "damlc": { "version": "$DAML_VERSION" },
      "daml-script": { "version": "$DAML_VERSION" },
      "codegen": { "version": "$DAML_VERSION" },
    }
  | delpaths([["sdk-version"]])
  ''' > $WORKDIR/$SOURCE_ROOT/daml.yaml

  # Append the name of the package to the multi-package.yaml
  echo "- ./$SOURCE_ROOT" >> $WORKDIR/multi-package.yaml
done

# Move staged changes into Canton
mkdir -p "$CANTON_DESTINATION"
find "$CANTON_DESTINATION" -mindepth 1 -maxdepth 1 -type d | xargs -d '\n' -- rm -rf
find "$CANTON_DESTINATION" -name multi-package.yaml | xargs -d '\n' -- rm -rf
cp -r "$WORKDIR"/* "$CANTON_DESTINATION"
rm -r "$WORKDIR"
