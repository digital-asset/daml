// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { record } from "./base/record";
import { RequiredValidation } from "./base/typelevel";
import { Filters } from "./filters";

function required(): RequiredValidation<ledger.TransactionFilter> {
    return {
        filtersByParty: record(Filters)
    };
}

export const TransactionFilter: Validation = object<ledger.TransactionFilter>('TransactionFilter', required, () => ({}));