:: Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@echo off
setlocal
cd %~dp0\..
stack exec --stack-yaml=rattle/stack.yaml sh rattle/build.sh
