// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { LedgerOffset } from "./ledger_offset";

function required(): RequiredValidation<ledger.GetLedgerEndResponse> {
    return {
        offset: LedgerOffset
    };
}

export const GetLedgerEndResponse: Validation = object<ledger.GetLedgerEndResponse>('GetLedgerEndResponse', required, () => ({}));