#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

cd $HOME/quickstart
da start &
sleep 15
echo "Press Ctrl-C to quit this."
sleep infinity
