:: Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@SETLOCAL
@SET /P DAML_VERSION=<daml_version.txt
%~dp0daml\daml install %~dp0 --install-assistant=yes --install-with-custom-version=%DAML_VERSION%
@ENDLOCAL
