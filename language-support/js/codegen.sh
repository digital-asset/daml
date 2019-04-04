# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"

bazel build //language-support/js/daml-codegen

mkdir -p daml-codegen/target/
tar xzf ../../bazel-genfiles/language-support/js/daml-codegen/daml-codegen.tgz --strip-components=1 -C daml-codegen/target

node daml-codegen/target/dist/index.js "$@"