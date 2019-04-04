// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { LedgerConfiguration } from "./ledger_configuration";

function required(): RequiredValidation<ledger.GetLedgerConfigurationResponse> {
    return {
        config: LedgerConfiguration
    };
}

export const GetLedgerConfigurationResponse: Validation = object<ledger.GetLedgerConfigurationResponse>('GetLedgerConfigurationResponse', required, () => ({}));