#!/bin/bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR"/../..
bazel run canton/community_app -- daemon -c ${DIR}/canton.conf --bootstrap ${DIR}/bootstrap.canton --debug --log-file-name=/tmp/canton.log
