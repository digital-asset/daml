# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# DADE shell profile for bash

export DADE_REPO_ROOT="$(cd $(dirname "${BASH_SOURCE[0]}")/.. ; pwd)"
source /dev/stdin <<< "$(${DADE_REPO_ROOT}/dev-env/bin/dade assist)"
