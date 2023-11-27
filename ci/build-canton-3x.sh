#!/bin/bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

eval "$(./dev-env/bin/dade-assist)"

tmp=$(mktemp -d)
trap 'rm -rf ${tmp}' EXIT

if [ -z "${GITHUB_TOKEN:-}" ]; then
  repo_url="git@github.com:DACH-NY/canton.git"
else
  repo_url="https://$GITHUB_TOKEN@github.com/DACH-NY/canton"
fi

git clone --depth 1 --branch main $repo_url $tmp
head=$(git -C $tmp rev-parse HEAD)
echo "cloned at revision $head"

daml_common_staging_src="$tmp/daml-common-staging"
community_src="$tmp/community"

daml_common_staging_dst="canton-3x/daml-common-staging"
community_dst="canton-3x/community"

# Clean-up existing dirs
rm -rf $daml_common_staging_dst $community_dst

# Copy files from the cloned Canton repo /community and exclude symlinks to Daml sources
rsync -a \
  --exclude="$community_src/participant/src/main/resources/ledger-api/VERSION" \
  --exclude="$community_src/lib/daml-copy-testing/sample-service-test-symlink/scala/com" \
  --exclude="$community_src/lib/daml-copy-testing/rs-grpc-pekko-test-symlink/scala/com" \
  --exclude="$community_src/lib/daml-copy-protobuf-java/protobuf-daml-symlinks/transaction/com" \
  --exclude="$community_src/lib/daml-copy-protobuf-java/protobuf-daml-symlinks/archive/com" \
  --exclude="$community_src/lib/daml-copy-testing-0/rs-grpc-bridge-test-symlink/java/com" \
  --exclude="$community_src/lib/daml-copy-testing-0/ledger-resources-test-symlink/scala/com" \
  --exclude="$community_src/lib/daml-copy-testing-0/protobuf-daml-symlinks/ledger-api-sample-service/hello.proto" \
  --exclude="$community_src/lib/daml-copy-testing-0/observability-metrics-test-symlink/scala/com" \
  --exclude="$community_src/lib/daml-copy-testing-0/observability-tracing-test-symlink/scala/com" \
  $community_src canton-3x

rsync -a $daml_common_staging_src canton-3x

git add $daml_common_staging_dst $community_dst

sed -i 's/canton-3x\///' .bazelignore
bazel build //canton-3x/...
