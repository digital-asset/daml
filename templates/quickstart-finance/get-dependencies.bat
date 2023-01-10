:: Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@echo off

:: Create .lib directory
if exist ".\.lib\" rmdir /s /q .lib
mkdir .\.lib

:: Get the dependency list
echo Downloading the list of dependencies
for /f "tokens=2" %%a IN ('findstr "^version" daml.yaml') DO (set version=%%a)

curl -Lf# "https://raw.githubusercontent.com/digital-asset/daml-finance/main/docs/code-samples/getting-started-config/%version%.conf" -o .lib/%version%.conf

for /F "tokens=*" %%a in (.lib/%version%.conf) do (
  for /F "tokens=1,2" %%b in ("%%a") do (
    echo Downloading: %%b to %%c
    curl -Lf# "%%b" -o %%c
  )
)
