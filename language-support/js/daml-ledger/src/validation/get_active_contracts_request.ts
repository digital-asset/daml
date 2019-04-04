// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation } from "./base";
import { RequiredValidation, OptionalValidation } from "./base/typelevel";
import { TransactionFilter } from "./transaction_filter";

function required(): RequiredValidation<ledger.GetActiveContractsRequest> {
    return {
        filter: TransactionFilter,
    };
}

function optional(): OptionalValidation<ledger.GetActiveContractsRequest> {
    return {
        verbose: native('boolean'),
    }
}

export const GetActiveContractsRequest: Validation = object<ledger.GetActiveContractsRequest>('GetActiveContractsRequest', required, optional);