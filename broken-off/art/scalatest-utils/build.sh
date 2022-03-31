#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [ -n "${WINDOWS:-}" ]; then
    # `dirname` doesn't seem to be available on Windows
    echo "WARNING: Running on Windows, guessing folders. Please run from script dir."
    DIR="$(pwd)"
else
    DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
fi

export VERSION=$1
TARGET_DIR="$2"

cd "$DIR"

http_url=https://storage.googleapis.com/daml-binaries/build-inputs
gs_url=gs://daml-binaries/build-inputs
file=scalatest-utils_2.13
endings=".jar .pom -sources.jar -javadoc.jar"
key=$(mktemp)
export CLOUDSDK_CONFIG=$(mktemp -d)
trap "rm -rf $key $CLOUDSDK_CONFIG" EXIT
export BOTO_CONFIG=/dev/null

if [ -f "$TARGET_DIR"/$file-$VERSION.jar ]; then
    echo "$file-$VERSION.jar already exists, no need to build."
elif curl --fail -I $http_url/$file-$VERSION.jar; then
    echo "$file-$VERSION exists in cache, downloading..."
    for ending in $endings; do
        curl $http_url/$file-$VERSION$ending > "$TARGET_DIR"/$file-$VERSION$ending
    done
else
    echo "Building $file from scratch..."
    export TEMP_MVN=$(mktemp -d)
    sbt compile
    sbt test
    sbt publish
    for ending in $endings; do
        cp "$TEMP_MVN"/com/daml/$file/$VERSION/$file-$VERSION$ending "$TARGET_DIR"/
    done
    if [ -n "${GOOGLE_APPLICATION_CREDENTIALS_CONTENT:-}" ]; then
        echo "GCloud credentials detected; pushing..."
        echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > $key
        gcloud auth activate-service-account --key-file=$key
        for ending in $endings; do
            gsutil cp "$TARGET_DIR"/$file-$VERSION$ending $gs_url/$file-$VERSION$ending
        done
    fi
fi
