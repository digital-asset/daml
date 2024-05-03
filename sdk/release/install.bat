:: Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@SETLOCAL EnableDelayedExpansion

@IF EXIST %~dp0daml_version.txt (
    SET /P DAML_VERSION=<%~dp0daml_version.txt
    SET CUSTOM_DAML_VERSION=--install-with-custom-version=!DAML_VERSION!
)

%~dp0daml\daml install %~dp0 --install-assistant=yes %CUSTOM_DAML_VERSION%

@ENDLOCAL
