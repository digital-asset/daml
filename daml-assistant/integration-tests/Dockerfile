# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
FROM debian:stretch

ENV USER daml

RUN set -eux;\
    apt-get update; \
    apt-get install --yes bsdmainutils ca-certificates netbase wget openjdk-8-jdk; \
    useradd -m -s /bin/bash $USER;
USER daml
COPY sdk-release-tarball.tar.gz /data/
COPY run-test.sh /data/
