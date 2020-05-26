#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


SCRIPT_DIR=$(dirname "$0")
cd $SCRIPT_DIR
BUILD_DIR=$(cd ..; pwd)/build

trap cleanup 1 2 3 6

cleanup()
{
  echo "Caught Signal ... cleaning up."
  rm -rf $BUILD_DIR
  cd $SCRIPT_DIR
  rm -rf ../source/daml/stdlib
  rm -f ../source/app-dev/grpc/proto-docs.rst
  rm -f ../source/LICENSE
  rm -f ../source/NOTICES
  echo "Done cleanup ... quitting."
  exit 1
}

rm -rf $BUILD_DIR
mkdir -p $BUILD_DIR/gen

ln -s ../source $BUILD_DIR
ln -s ../configs $BUILD_DIR
mkdir $BUILD_DIR/theme
bazel build //docs:theme
tar -zxf ../../bazel-bin/docs/da_theme.tar.gz -C $BUILD_DIR/theme

# License and Notices
cp ../../LICENSE ../source
cp ../../NOTICES ../source


for arg in "$@"
do
    if [ "$arg" = "--pdf" ]; then
        bazel build //docs:pdf-docs
        mkdir -p $BUILD_DIR/gen/_downloads
        cp -L ../../bazel-bin/docs/DigitalAssetSDK.pdf $BUILD_DIR/gen/_downloads
    fi
    if [ "$arg" = "--gen" ]; then
        # Hoogle
        bazel build //compiler/damlc:daml-base-hoogle.txt
        mkdir -p $BUILD_DIR/gen/hoogle_db
        cp -L ../../bazel-bin/compiler/damlc/daml-base-hoogle.txt $BUILD_DIR/gen/hoogle_db/base.txt

        # Javadoc
        bazel build //language-support/java:javadoc
        mkdir -p $BUILD_DIR/gen/app-dev/bindings-java
        unzip ../../bazel-bin/language-support/java/javadoc.jar -d $BUILD_DIR/gen/app-dev/bindings-java/javadocs/

        # Proto-docs
        bazel build //ledger-api/grpc-definitions:docs
        cp -L ../../bazel-bin/ledger-api/grpc-definitions/proto-docs.rst ../source/app-dev/grpc/

        #StdLib
        bazel build //compiler/damlc:daml-base-rst.tar.gz
        tar xf ../../bazel-bin/compiler/damlc/daml-base-rst.tar.gz \
            --strip-components 1 -C ../source/daml/stdlib
    fi
done

DATE=$(date +"%Y-%m-%d")
echo { \"$DATE\" : \"$DATE\" } >  $BUILD_DIR/gen/versions.json

pipenv install
pipenv run sphinx-autobuild -c $BUILD_DIR/configs/html $BUILD_DIR/source $BUILD_DIR/gen
