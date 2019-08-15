:: Copyright (c) 2019 The DAML Authors. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@echo off

for /f %%i in ('git rev-parse HEAD') do set RESULT=%%i
echo STABLE_GIT_REVISION %RESULT%
