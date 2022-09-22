#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail


SCRIPT_DIR=$PWD/$(dirname "$0")
cd $SCRIPT_DIR
BUILD_DIR=$(cd ..; pwd)/build
BAZEL_BIN="$SCRIPT_DIR/../../bazel-bin"

trap cleanup 1 2 3 6

cleanup()
{
  echo "Caught Signal ... cleaning up."
  rm -rf $TEMPLATES_DIR/
  rm -rf $BUILD_DIR
  cd $SCRIPT_DIR
  rm -rf ../source/getting-started/code
  rm -rf ../source/daml/stdlib
  rm -f ../source/app-dev/grpc/proto-docs.rst
  rm -f ../source/app-dev/grpc/error-codes-inventory.rst.inc
  rm -f ../source/app-dev/grpc/error-categories-inventory.rst.inc
  rm -f ../source/LICENSE
  rm -f ../source/NOTICES
  rm -f ../source/index.rst
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

# GSG source, while this is arguably part of --gen
# it is quick enough and important enough to always include it.
bazel build //templates:create-daml-app-docs
mkdir -p $BUILD_DIR/source/getting-started/code/templates-tarball
tar -zxf ../../bazel-bin/templates/create-daml-app-docs.tar.gz -C $BUILD_DIR/source/getting-started/code/templates-tarball/

# Templates
bazel build //templates:templates-tarball
TEMPLATES_DIR=$BUILD_DIR/source/_templates
mkdir -p $TEMPLATES_DIR
tar -zxf $BAZEL_BIN/templates/templates-tarball.tar.gz -C $TEMPLATES_DIR --strip-components=1

# Index
bazel build //docs:index
cp $BAZEL_BIN/docs/index.rst $BUILD_DIR/source/index.rst

GEN_ERROR_CODES=false

for arg in "$@"
do
    if [ "$arg" = "--pdf" ]; then
        bazel build //docs:pdf-docs
        mkdir -p $BUILD_DIR/gen/_downloads
        cp -L ../../bazel-bin/docs/DigitalAssetSDK.pdf $BUILD_DIR/gen/_downloads
    fi
    if [ "$arg" = "--gen" ]; then
        # NOTE:
        # $BUILD_DIR/source is a symlink into the versioned controlled directory with source .rst files.
        # When generating files into that directory make sure to remove them before this script ends.

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
        mkdir -p ../source/daml/stdlib
        tar xf ../../bazel-bin/compiler/damlc/daml-base-rst.tar.gz \
            --strip-components 1 -C ../source/daml/stdlib

    fi
    if  [ "$arg" = "--gen" ] || [ "$arg" = "--gen-error-codes" ]; then
      GEN_ERROR_CODES=true
    fi
done

if  [ "$GEN_ERROR_CODES" = "true" ]; then
    # Error codes and error categories
    ./gen-error-docs-src.sh
    GEN_ERROR_PAGES_DIR=$(cd ..; pwd)/resources/generated-error-pages
    cp -L $GEN_ERROR_PAGES_DIR/error-codes-inventory.rst.inc      $BUILD_DIR/source/app-dev/grpc/error-codes-inventory.rst.inc
    cp -L $GEN_ERROR_PAGES_DIR/error-categories-inventory.rst.inc $BUILD_DIR/source/app-dev/grpc/error-categories-inventory.rst.inc
fi

DATE=$(date +"%Y-%m-%d")
echo { \"$DATE\" : \"$DATE\" } >  $BUILD_DIR/gen/versions.json

pipenv install
pipenv run sphinx-autobuild -c $BUILD_DIR/configs/html $BUILD_DIR/source $BUILD_DIR/gen
