:: Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

:: Copyright (c) 2019, Digital Asset (Switzerland) GmbH and/or its affiliates.
:: All rights reserved.

for %%i in ("%~dp0..") do (
  java -jar %%i %*
  goto finish
)

:finish
