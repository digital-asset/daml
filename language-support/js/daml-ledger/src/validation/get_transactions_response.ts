// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, array } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Transaction } from "./transaction";

function required(): RequiredValidation<ledger.GetTransactionsResponse> {
    return {
        transactions: array(Transaction),
    };
}

export const GetTransactionsResponse: Validation = object<ledger.GetTransactionsResponse>('GetTransactionsResponse', required, () => ({}));