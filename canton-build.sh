#!/usr/bin/env bash
set -euo pipefail

where=${1:-${PWD}}
url=${2:-"git@github.com:DACH-NY/canton.git"}

deps="${where}/arbitrary_canton_sha
 ${where}/maven_install_2.13.json
 ${where}/observability
 ${where}/daml-lf
 ${where}/libs-scala
 ${where}/ledger-api
 ${where}/language-support
"

set -euo pipefail
mkdir -p "${where}/canton_lib"
if [ -f arbitrary_canton_sha ]; then
  sha=$(find $deps -type f | xargs sha256sum  | sha256sum | cut -b -64)
  if [[ ! -f "${where}/canton_lib/${sha}.jar" ]]; then
    commit=$(cat arbitrary_canton_sha)
    cd "${where}/canton_lib"
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
    cp community/app/target/scala-*/canton-open-source-*.jar "${where}/canton_lib/${sha}.jar"
  fi
  ln -sn "${where}/canton_lib/${sha}.jar" "${where}/canton_lib/canton.jar"
fi
