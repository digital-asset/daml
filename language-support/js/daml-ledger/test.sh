#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -ex

export PATH=$PATH:external/nodejs/bin

cp -LR external/npm/node_modules .

# simulate `npm install daml-grpc` -- sh_test makes sure transitive deps are there
mkdir -p node_modules/daml-grpc
tar xzf language-support/js/daml-grpc/daml-grpc.tgz --strip-components=1 -C node_modules/daml-grpc

# Resolve the symbolic link to have the test import statements point to
# the generated code folder so that the dependencies on generated code can
# be resolved
cp -LR language-support/js/daml-ledger/src .
cp -LR language-support/js/daml-ledger/tests .

echo '{"compilerOptions":{"lib":["es2015"]}}' > tsconfig.json

node_modules/mocha/bin/mocha -r ts-node/register --recursive 'tests/**/*.ts'