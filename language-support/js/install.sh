# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"

bazel build //language-support/js/daml-grpc
bazel build //language-support/js/daml-ledger

mkdir -p daml-ledger/node_modules/daml-grpc
tar xzf ../../bazel-genfiles/language-support/js/daml-grpc/daml-grpc.tgz --strip-components=1 -C daml-ledger/node_modules/daml-grpc

mkdir -p daml-codegen/node_modules/daml-grpc
tar xzf ../../bazel-genfiles/language-support/js/daml-grpc/daml-grpc.tgz --strip-components=1 -C daml-codegen/node_modules/daml-grpc

mkdir -p daml-codegen/node_modules/daml-ledger
tar xzf ../../bazel-genfiles/language-support/js/daml-ledger/daml-ledger.tgz --strip-components=1 -C daml-codegen/node_modules/daml-ledger