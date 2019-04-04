// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, array } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { TransactionTree } from "./transaction_tree";

function required(): RequiredValidation<ledger.GetTransactionTreesResponse> {
    return {
        transactions: array(TransactionTree),
    };
}

export const GetTransactionTreesResponse: Validation = object<ledger.GetTransactionTreesResponse>('GetTransactionTreesResponse', required, () => ({}));