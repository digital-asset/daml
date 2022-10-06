:: Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
:: SPDX-License-Identifier: Apache-2.0

@echo off

:: Target Daml Finance version
set version=0.1.3

:: Create .lib directory if it doesn't exist
if not exist ".\.lib\" mkdir .\.lib

if not exist ".lib/daml-finance-holding-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Holding/%version%/daml-finance-holding-%version%.dar" -o .lib/daml-finance-holding-%version%.dar )
if not exist ".lib/daml-finance-holding-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Account/%version%/daml-finance-acount-%version%.dar" -o .lib/daml-finance-account-%version%.dar )
if not exist ".lib/daml-finance-instrument-token-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Instrument.Token/%version%/daml-finance-instrument-token-%version%.dar" -o .lib/daml-finance-instrument-token-%version%.dar )
if not exist ".lib/daml-finance-interface-lifecycle-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Lifecycle/%version%/daml-finance-interface-lifecycle-%version%.dar" -o .lib/daml-finance-interface-lifecycle-%version%.dar )
if not exist ".lib/daml-finance-interface-holding-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Holding/%version%/daml-finance-interface-holding-%version%.dar" -o .lib/daml-finance-interface-holding-%version%.dar )
if not exist ".lib/daml-finance-interface-account-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Account/%version%/daml-finance-interface-account-%version%.dar" -o .lib/daml-finance-interface-account-%version%.dar )
if not exist ".lib/daml-finance-interface-instrument-base-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Instrument.Base/%version%/daml-finance-interface-instrument-base-%version%.dar" -o .lib/daml-finance-interface-instrument-base-%version%.dar )
if not exist ".lib/daml-finance-interface-settlement-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Settlement/%version%/daml-finance-interface-settlement-%version%.dar" -o .lib/daml-finance-interface-settlement-%version%.dar )
if not exist ".lib/daml-finance-interface-types-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Types/%version%/daml-finance-interface-types-%version%.dar" -o .lib/daml-finance-interface-types-%version%.dar )
if not exist ".lib/daml-finance-lifecycle-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Lifecycle/%version%/daml-finance-lifecycle-%version%.dar" -o .lib/daml-finance-lifecycle-%version%.dar )
if not exist ".lib/daml-finance-data-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Data/%version%/daml-finance-data-%version%.dar" -o .lib/daml-finance-data-%version%.dar )
if not exist ".lib/daml-finance-settlement-%version%.dar" ( curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Settlement/%version%/daml-finance-settlement-%version%.dar" -o .lib/daml-finance-settlement-%version%.dar )
