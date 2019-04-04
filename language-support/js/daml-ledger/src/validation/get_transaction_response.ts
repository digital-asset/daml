// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { TransactionTree } from "./transaction_tree";

function required(): RequiredValidation<ledger.GetTransactionResponse> {
    return {
        transaction: TransactionTree,
    };
}

export const GetTransactionResponse: Validation = object<ledger.GetTransactionResponse>('GetTransactionResponse', required, () => ({}));