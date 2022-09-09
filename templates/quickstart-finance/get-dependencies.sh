#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

# Target Daml Finance version
version="0.1.3"

# Daml Finance dependent libraries
dependencies=( \
  "daml-finance-holding" \
  "daml-finance-instrument-base" \
  "daml-finance-interface-lifecycle" \
  "daml-finance-interface-holding" \
  "daml-finance-interface-instrument-base" \
  "daml-finance-interface-settlement" \
  "daml-finance-interface-types" \
  "daml-finance-lifecycle" \
  "daml-finance-refdata" \
  "daml-finance-settlement" \
)

# Create .lib directory if it doesn't exist
if [[ ! -d .lib ]]; then
  mkdir .lib
fi

if [[ ! -a ".lib/daml-finance-holding-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Holding/0.1.3/daml-finance-holding-0.1.3.dar" -o .lib/daml-finance-holding-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-instrument-base-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Instrument.Base/0.1.3/daml-finance-instrument-base-0.1.3.dar" -o .lib/daml-finance-instrument-base-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-interface-lifecycle-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Lifecycle/0.1.3/daml-finance-interface-lifecycle-0.1.3.dar" -o .lib/daml-finance-interface-lifecycle-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-interface-holding-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Holding/0.1.3/daml-finance-interface-holding-0.1.3.dar" -o .lib/daml-finance-interface-holding-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-interface-instrument-base-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Instrument.Base/0.1.3/daml-finance-interface-instrument-base-0.1.3.dar" -o .lib/daml-finance-interface-instrument-base-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-interface-settlement-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Settlement/0.1.3/daml-finance-interface-settlement-0.1.3.dar" -o .lib/daml-finance-interface-settlement-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-interface-types-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Types/0.1.3/daml-finance-interface-types-0.1.3.dar" -o .lib/daml-finance-interface-types-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-lifecycle-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Lifecycle/0.1.3/daml-finance-lifecycle-0.1.3.dar" -o .lib/daml-finance-lifecycle-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-refdata-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.RefData/0.1.3/daml-finance-refdata-0.1.3.dar" -o .lib/daml-finance-refdata-0.1.3.dar; fi
if [[ ! -a ".lib/daml-finance-settlement-0.1.3.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Settlement/0.1.3/daml-finance-settlement-0.1.3.dar" -o .lib/daml-finance-settlement-0.1.3.dar; fi
