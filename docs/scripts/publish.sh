#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Needs to be run with the `aws` command available and
# after running `aws-google-auth`.

export AWS_PROFILE=sts

SCRIPT_DIR=$(dirname "$0")
cd $SCRIPT_DIR

STAGE=false

for arg in "$@"
do
  if [ "$arg" = "--stage" ]; then
    STAGE=true
  fi
done

if [ $STAGE == true ]; then
  BUCKET=s3://docs-daml-com-staging/
else
  BUCKET=s3://docs-daml-com/
fi

bazel build //docs
tar -zxf ../../bazel-genfiles/docs/html.tar.gz

aws s3 sync \
  html \
  $BUCKET \
  --delete \
  --acl public-read \
  --exclude .doctrees/** \
  --exclude .buildinfo

rm -r html

if [ $STAGE == false ]; then
  aws cloudfront create-invalidation --distribution-id E1U753I56ERH55 --paths '/*'
fi
