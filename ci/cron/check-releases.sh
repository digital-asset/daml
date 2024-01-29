#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

MAX_RELEASES=${MAX_RELEASES:-}
AUTH=${AUTH:-"Authorization: token $GITHUB_TOKEN"}
USER_AGENT="User-Agent: Daml cron (gary.verhaegen@digital-asset.com)"

export GNUPGHOME=$(mktemp -d)
trap "rm -rf $GNUPGHOME" EXIT

setup_gpg() (
    key=$DIR/../pgp_pubkey
    log=$(mktemp)
    trap "rm -rf $log" EXIT
    if ! gpg --no-tty --quiet --import $key >>$log 2>&1; then
        echo "Failed to initialize GPG. Logs:"
        echo "---"
        cat $log
        echo "---"
        exit 1
    fi
)

get_releases() (
    url="https://api.github.com/repos/digital-asset/daml/releases"
    tmp_dir=$(mktemp -d)
    trap "cd; rm -rf $tmp_dir" EXIT
    cd $tmp_dir
    while [ "$url" != "" ]; do
        curl $url \
             --fail \
             --silent \
             -H "$AUTH" \
             -H "$USER_AGENT" \
             -o >(cat - \
                 | jq -c '.[]
                          | { prerelease,
                              tag: .tag_name[1:],
                              assets: [.assets[] | .browser_download_url] }' \
                 >> resp) \
             -D headers
        url=$(cat headers \
              | tr -d '\r' \
              | grep "link:" \
              | grep -Po '(?<=<)([^>]*)(?=>; rel="next")' \
              || true)
    done
    cat resp
)

retry() (
    attempts=$1
    cmd=$2
    cont=1
    delay=10
    while [ $cont == 1 ]; do
        if $cmd; then
            cont=0
        else
            echo "    Exit $? for '$cmd', retrying."
            sleep $delay
            delay=$(( delay + delay ))
            attempts=$((attempts - 1))
            if [ "$attempts" = 0 ]; then
                echo "Max retries reached. Giving up on '$cmd'."
                exit 1
            fi
        fi
    done
)

download_assets() (
    release=$1
    curl -H "$AUTH" \
         -H "$USER_AGENT" \
         --silent \
         --fail \
         --location \
         --parallel \
         --remote-name-all \
         $(echo $release | jq -r '.assets[]')
)

verify_signatures() (
    log=$(mktemp)
    trap "rm -f $log" EXIT
    for f in $(ls | grep -v '\.asc$'); do
        if ! test -f $f.asc; then
            echo "No signature file on GitHub for $f."
            exit 1
        fi
        if ! gpg --verify $f.asc &>$log; then
            echo "Failed to verify signature for $f."
            echo "gpg logs:"
            echo "---"
            cat $log
            echo "---"
            exit 1
        fi
    done
)

verify_backup() (
    tag=$1
    gcs_base=gs://daml-data/releases/${tag#v}/github
    log=$(mktemp)
    trap "rm -f $log" EXIT
    for f in $(ls); do
        (
            if ! gsutil ls $gcs_base/$f &>/dev/null; then
                echo "No backup for $f; aborting."
                exit 1
            else
                gsutil cp $gcs_base/$f $f.gcs &>$log
                if ! diff $f $f.gcs; then
                    echo "$f does not match backup; aborting."
                    echo "gcs copy output:"
                    echo "---"
                    cat $log
                    echo "---"
                    exit 1
                fi
            fi
         ) &
    done
    for pid in $(jobs -p); do
        wait $pid
    done
)

check_release() (
    release=$1
    tag=$(echo $release | jq -r .tag)
    tmp_dir=$(mktemp -d)
    trap "cd; rm -rf $tmp_dir" EXIT
    cd $tmp_dir
    retry 5 "download_assets $release"
    verify_signatures
    verify_backup $tag
)

create_record() (
  listing=$1
  release=$2
  gcloud_resp=$(mktemp)
  gcloud storage ls "gs://daml-data/releases/$release/**/*" --json \
    > $gcloud_resp
  github_resp=$(mktemp)
  curl --fail \
       --silent \
       --location \
       -H "$AUTH" \
       -H "$USER_AGENT" \
       -H "X-GitHub-Api-Version: 2022-11-28" \
       "https://api.github.com/repos/digital-asset/daml/releases/tags/v$release" \
    | jq '[.assets[] | {name, created_at, updated_at, uploader: .uploader.login}]' \
    > $github_resp
  jq -n '{gcloud: input, github: input}' $gcloud_resp $github_resp > $listing
)

remote_record_location() (
  release=$1
  echo "gs://daml-data/checked-releases/$release"
)

has_record() (
  release=$1
  gcloud storage ls $(remote_record_location $release) &>/dev/null
)

matches_record() (
  listing=$1
  release=$2
  remote=$(mktemp)
  gcloud storage cat $(remote_record_location $release) > $remote
  diff $remote $listing
)

record_success() (
  record_before=$1
  release=$2
  record_after=$3
  create_record $record_after $release
  if diff $record_before $record_after; then
    echo "[$(date --date=@$SECONDS -u +%H:%M:%S)] $tag: saving record."
    gsutil -q cp $record_before $(remote_record_location $release)
  else
    echo "[$(date --date=@$SECONDS -u +%H:%M:%S)] $tag: artifacts have changed while verifying."
    exit 1
  fi
)

main() (
  setup_gpg
  releases=$(get_releases)

  if [ "" != "$MAX_RELEASES" ]; then
    releases=$( (echo "$releases" | head -n $MAX_RELEASES) || test $? -eq 141)
  fi

  for r in $releases; do
    listing=$(mktemp)
    tag=$(echo $r | jq -r .tag)
    create_record $listing $tag
    if has_record $tag; then
      if matches_record $listing $tag; then
        echo "[$(date --date=@$SECONDS -u +%H:%M:%S)] $tag: matches record, skipping."
      else
        echo "[$(date --date=@$SECONDS -u +%H:%M:%S)] $tag: does not match records, see above for differences."
      fi
    else
      echo "[$(date --date=@$SECONDS -u +%H:%M:%S)] $tag: verifying."
      check_release $r
      record_success $listing $tag
    fi
  done
)

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
