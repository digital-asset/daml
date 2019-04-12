:: Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@echo off

for /f %%i in ('git rev-parse HEAD') do set RESULT=%%i
echo STABLE_GIT_REVISION %RESULT%
