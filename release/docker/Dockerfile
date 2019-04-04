# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
FROM ubuntu:16.04

ENV USER daml

RUN set -eux;\
    apt-get update; \
    apt-get install --yes sudo bsdmainutils ca-certificates netbase wget openjdk-8-jdk; \
    useradd -m -s /bin/bash -G sudo $USER; \
    echo "$USER ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers ; \
    # `da start` calls `xdg-open`, which does not make much sense in a container.
    ln -s /bin/true /usr/local/bin/xdg-open

ENV DA_CLI_VERSION 112-b616cb073d
ENV DA_CLI da-cli-$DA_CLI_VERSION-linux.run

USER $USER
WORKDIR /home/$USER

RUN set -eux; \
    wget https://digitalassetsdk.bintray.com/DigitalAssetSDK/com/digitalasset/da-cli/$DA_CLI_VERSION/$DA_CLI; \
    sh $DA_CLI; \
    rm $DA_CLI; \
    sudo ln -s $HOME/.da/bin/da /usr/local/bin/da; \
    da setup

ENV MAIN daml/Main.daml
RUN set -eux; \
    da new quickstart-java quickstart; \
    cd quickstart; \
    da run damlc -- test $MAIN

ENV START start.sh
COPY $START $START
RUN set -eux; \
    sudo chown $USER:$USER $START; \
    chmod u+x $START

EXPOSE 7500
EXPOSE 6865

CMD ./$START
