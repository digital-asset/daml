#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

# Target Daml Finance version
version="0.1.3"

# Create .lib directory if it doesn't exist
if [[ ! -d .lib ]]; then
  mkdir .lib
fi

if [[ ! -a ".lib/daml-finance-holding-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Holding/${version}/daml-finance-holding-${version}.dar" -o .lib/daml-finance-holding-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-instrument-base-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Instrument.Base/${version}/daml-finance-instrument-base-${version}.dar" -o .lib/daml-finance-instrument-base-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-interface-lifecycle-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Lifecycle/${version}/daml-finance-interface-lifecycle-${version}.dar" -o .lib/daml-finance-interface-lifecycle-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-interface-holding-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Holding/${version}/daml-finance-interface-holding-${version}.dar" -o .lib/daml-finance-interface-holding-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-interface-instrument-base-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Instrument.Base/${version}/daml-finance-interface-instrument-base-${version}.dar" -o .lib/daml-finance-interface-instrument-base-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-interface-settlement-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Settlement/${version}/daml-finance-interface-settlement-${version}.dar" -o .lib/daml-finance-interface-settlement-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-interface-types-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Interface.Types/${version}/daml-finance-interface-types-${version}.dar" -o .lib/daml-finance-interface-types-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-lifecycle-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Lifecycle/${version}/daml-finance-lifecycle-${version}.dar" -o .lib/daml-finance-lifecycle-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-refdata-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.RefData/${version}/daml-finance-refdata-${version}.dar" -o .lib/daml-finance-refdata-${version}.dar; fi
if [[ ! -a ".lib/daml-finance-settlement-${version}.dar" ]]; then curl -Lf# "https://github.com/digital-asset/daml-finance/releases/download/Daml.Finance.Settlement/${version}/daml-finance-settlement-${version}.dar" -o .lib/daml-finance-settlement-${version}.dar; fi
