#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

cd "$(dirname "$0")/.."
stack exec --package=shake --package=filepattern --stack-yaml=rattle/stack.yaml -- runhaskell -irattle rattle/MainScala.hs
