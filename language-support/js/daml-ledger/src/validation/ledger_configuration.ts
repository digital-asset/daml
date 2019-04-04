// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Duration } from "./duration";

function required(): RequiredValidation<ledger.LedgerConfiguration> {
    return {
        maxTtl: Duration,
        minTtl: Duration
    };
}

export const LedgerConfiguration: Validation = object<ledger.LedgerConfiguration>('LedgerConfiguration', required, () => ({}));