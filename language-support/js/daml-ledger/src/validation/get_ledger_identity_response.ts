// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, native } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.GetLedgerIdentityResponse> {
    return {
        ledgerId: native('string')
    };
}

export const GetLedgerIdentityResponse: Validation = object<ledger.GetLedgerIdentityResponse>('GetLedgerIdentityResponse', required, () => ({}));