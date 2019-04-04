// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, native, object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.GetTransactionByIdRequest> {
    return {
        transactionId: native('string'),
        requestingParties: array(native('string')),
    };
}

export const GetTransactionByIdRequest: Validation = object<ledger.GetTransactionByIdRequest>('GetTransactionByIdRequest', required, () => ({}));