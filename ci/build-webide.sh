#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

[ -z $1 ] && echo "usage build-webide [version] [latest:defaults true] [test:defaults true]" && exit 1

echo "building webide image with $@"
VERSION=$1
LATEST=${2:-true}
TEST=${3:-true}
PUSH=${4:-true}
DOCKER_FILE="$(dirname "${BASH_SOURCE[0]}")/../web-ide/ide-server/"

fail() {
    stopDocker
    exit 1
}

ensureDockerStarted() {
    local CONTAINER_ID=$1
    local limit=10
    local i=0
    local RESULT="START"
    while [ $i -lt $limit -a $RESULT != "Up" ]; do
        sleep 1
        i=$(($i+1))
        RESULT=$(docker container ls --filter=id=$CONTAINER_ID --format {{.Status}} | cut -c1-2)
    done
    if [ $i -eq $limit ]; then
        echo "Could not start docker container: ls result='$RESULT'"
        fail
    fi
}

ensureWebideStarted() {
    echo "ensuring endpoint is responsive"
    local limit=10
    local i=0
    local RESULT=1
    which curl > /dev/null 2>&1 #ensure dev-env does its thing before sending request
    while [ $i -lt $limit -a $RESULT -ne 0 ]; do
        sleep 1
        i=$(($i+1))
        curl -I http://localhost:8443
        RESULT=$?
    done
    if [ $RESULT -ne 0 ]; then
        echo "Could not start webide"
        fail
    fi
}

stopDocker() {
    echo "stopping docker"
    docker rm -f daml-webide-test
}

which docker > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "docker not installed!"
    exit 1
fi

if [ "$LATEST" = true ]; then
    docker build --rm --build-arg DAML_VERSION=$VERSION -t gcr.io/da-gcp-web-ide-project/daml-webide:$VERSION -t gcr.io/da-gcp-web-ide-project/daml-webide:latest $DOCKER_FILE
else
    docker build --rm --build-arg DAML_VERSION=$VERSION  -t gcr.io/da-gcp-web-ide-project/daml-webide:$VERSION $DOCKER_FILE
fi

if [ "$TEST" = true ]; then
    IMAGE_ID=$(docker image ls --filter=reference="gcr.io/da-gcp-web-ide-project/daml-webide:$VERSION" --format "{{.ID}}")
    echo "running container for image $IMAGE_ID"
    CONTAINER_ID=$(docker run --name daml-webide-test --rm -d -p 8443:8443 $IMAGE_ID)
    sleep 5

    ensureDockerStarted $CONTAINER_ID
    ensureWebideStarted

    echo "!!!!!!!!!!! Test passed !!!!!!!!!!!!!!!!!"
    stopDocker
fi

if [ "$PUSH" = true ]; then
    docker push gcr.io/da-gcp-web-ide-project/daml-webide:$VERSION
    if [ "$LATEST" = true ]; then
        docker push gcr.io/da-gcp-web-ide-project/daml-webide:latest
    fi
fi

exit 0