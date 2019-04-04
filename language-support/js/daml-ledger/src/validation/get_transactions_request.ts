// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, native } from "./base";
import { OptionalValidation, RequiredValidation } from "./base/typelevel";
import { LedgerOffset } from "./ledger_offset";
import { TransactionFilter } from "./transaction_filter";

function required(): RequiredValidation<ledger.GetTransactionsRequest> {
    return {
        begin: LedgerOffset,
        filter: TransactionFilter
    };
}

function optional(): OptionalValidation<ledger.GetTransactionsRequest> {
    return {
        end: LedgerOffset,
        verbose: native('boolean')
    };
}

export const GetTransactionsRequest: Validation = object<ledger.GetTransactionsRequest>('GetTransactionsRequest', required, optional);