#!/bin/bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

￼
set -e

cd upgrade-issue
daml-head clean --all
daml-head build --all
cd repro
daml-head script --ledger-host=localhost --ledger-port=6865 --dar=.daml/dist/repro-0.0.1.dar --all --upload-dar=yes

