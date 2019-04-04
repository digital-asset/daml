// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, native, object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.GetTransactionByEventIdRequest> {
    return {
        eventId: native('string'),
        requestingParties: array(native('string')),
    };
}

export const GetTransactionByEventIdRequest: Validation = object<ledger.GetTransactionByEventIdRequest>('GetTransactionByEventIdRequest', required, () => ({}));