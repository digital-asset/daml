#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -ex

export PATH=$PATH:external/nodejs/bin:external/npm/node_modules/mocha/bin
ln -s external/npm/node_modules

mkdir -p node_modules/daml-grpc
tar xzf language-support/js/daml-grpc/daml-grpc.tgz --strip-components=1 -C node_modules/daml-grpc

echo '{"compilerOptions":{"lib":["es2015"]}}' > tsconfig.json

# Resolve the symbolic link to have the test import statements point to
# the generated code folder so that the dependencies on generated code can
# be resolved
cp -L language-support/js/daml-grpc/test.ts .

mocha -r ts-node/register test.ts