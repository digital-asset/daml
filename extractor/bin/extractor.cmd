:: Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

for %%i in ("%~dp0..") do (
  java -jar %%i %*
  goto finish
)

:finish
