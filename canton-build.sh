# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env bash

set -euo pipefail

root=$PWD
builingDir=${1:-${root}/canton/local_build}
stagingDir=${2:-${builingDir}}
url=${3:-"git@github.com:DACH-NY/canton.git"}

deps="${root}/arbitrary_canton_sha
 ${root}/maven_install_2.13.json
 ${root}/observability
 ${root}/daml-lf
 ${root}/libs-scala
 ${root}/ledger-api
 ${root}/language-support
"

set -euo pipefail
mkdir -p ${builingDir} ${stagingDir}
if [ -f arbitrary_canton_sha ]; then
  sha=$(find $deps -type f | xargs sha256sum  | sha256sum | cut -b -64)
  if [[ ! -f "${stagingDir}/lib/${sha}.jar" ]]; then
    commit=$(cat arbitrary_canton_sha)
    cd ${builingDir}
    if [[ ! -d canton ]]; then
        mkdir canton
        (cd canton && git init && git remote add origin $url)
    fi
    cd canton
      git fetch --depth 1 origin ${commit}
      git reset --hard ${commit}
      sed -i 's|git@github.com:|https://github.com/|' .gitmodules
      for submodule in 3rdparty/fuzzdb; do
        git submodule init ${submodule}
        git submodule update ${submodule}
      done
      rsync -ah --delete ${deps} daml/
      nix-shell --max-jobs 2 --run "sbt community-app/assembly"
    cd ..
    cp canton/community/app/target/scala-*/canton-open-source-*.jar ${sha}.jar
    rm -f ${stagingDir}/canton.jar
    ln -s ${sha}.jar ${stagingDir}/canton.jar
  fi
fi

