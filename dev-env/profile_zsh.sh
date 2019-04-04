# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# DADE shell profile compatible with zsh.

export DADE_REPO_ROOT="$(cd $(dirname "${(%):-%N}")/.. > /dev/null && pwd)"
source /dev/stdin <<< "$(${DADE_REPO_ROOT}/dev-env/bin/dade assist)"
