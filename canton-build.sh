#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

root=$PWD
stagingDir=${1:-${root}/canton}
url=${2:-"git@github.com:DACH-NY/canton.git"}

deps="${root}/arbitrary_canton_sha
 ${root}/maven_install_2.13.json
 ${root}/observability
 ${root}/daml-lf
 ${root}/libs-scala
 ${root}/ledger-api
 ${root}/language-support
"

set -euo pipefail
mkdir -p "${stagingDir}/local_canton_build"
if [ -f arbitrary_canton_sha ]; then
  sha=$(find $deps -type f | xargs sha256sum  | sha256sum | cut -b -64)
  if [[ ! -f "${stagingDir}/lib/${sha}.jar" ]]; then
    commit=$(cat arbitrary_canton_sha)
    cd "${stagingDir}/local_canton_build"
    if [[ ! -d canton ]]; then
      git clone $url canton
    fi
    cd canton
    git fetch
    git reset --hard ${commit}
    sed -i 's|git@github.com:|https://github.com/|' .gitmodules
    for submodule in 3rdparty/fuzzdb; do
      git submodule init ${submodule}
      git submodule update ${submodule}
    done
    rsync -avh --delete ${deps} daml/

    nix-shell --max-jobs 2 --run "sbt community-app/assembly"
    mkdir -p ${root}/canton/lib
    cp community/app/target/scala-*/canton-open-source-*.jar "${stagingDir}/lib/${sha}.jar"
  fi
  rm -f "${stagingDir}/lib/local-canton.jar"
  ln -s "${stagingDir}/lib/${sha}.jar" "${stagingDir}/lib/local-canton.jar"
fi

