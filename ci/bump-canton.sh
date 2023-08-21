# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( dirname $( readlink -f "${BASH_SOURCE[0]}" ) )"
cd "$DIR"/..

AUTH=${AUTH:-$ARTIFACTORY_USERNAME:$ARTIFACTORY_PASSWORD}

url_sha() (
  url="$1"
  curl -u $AUTH \
       --fail \
       --location \
       --silent \
       "$url" \
   | sha256sum \
   | awk '{print $1}'
)

canton_version=$(curl -u $AUTH \
                      --fail \
                      --location \
                      --silent \
                      https://digitalasset.jfrog.io/artifactory/api/storage/assembly/canton \
                 | jq -r '.children[].uri' \
                 | sed -e 's/^\///' \
                 | grep -P '^\d+\.\d+\.\d+' \
                 | sort -V \
                 | tail -1)
canton_url="https://digitalasset.jfrog.io/artifactory/assembly/canton/$canton_version/canton-open-source-$canton_version.tar.gz"
canton_sha=$(url_sha "$canton_url")

sed -i 's|SKIP_DEV_CANTON_TESTS=.*|SKIP_DEV_CANTON_TESTS=false|' build.sh
sed -e 's/^/# /' COPY > canton_dep.bzl
cat <<EOF >> canton_dep.bzl

canton = {
    "sha": "$canton_sha",
    "url": "https://www.canton.io/releases/canton-open-source-$canton_version.tar.gz",
    "local": False,
}
EOF

ee_canton_url="https://digitalasset.jfrog.io/artifactory/assembly/canton/$canton_version/canton-enterprise-$canton_version.tar.gz"
ee_canton_sha=$(url_sha "$ee_canton_url")

sed -i "s|CANTON_ENTERPRISE_VERSION=.*|CANTON_ENTERPRISE_VERSION=$canton_version|" canton/BUILD.bazel
sed -i "s|CANTON_ENTERPRISE_SHA=.*|CANTON_ENTERPRISE_SHA=$ee_canton_sha|" canton/BUILD.bazel

rm -f arbitrary_canton_sha

echo $canton_version
