// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation, array } from "./base";
import { RequiredValidation, OptionalValidation } from "./base/typelevel";
import { Timestamp } from "./timestamp";
import { Event } from "./event";

function required(): RequiredValidation<ledger.Transaction> {
    return {
        effectiveAt: Timestamp,
        events: array(Event),
        offset: native('string'),
        transactionId: native('string')
    };
}

function optional(): OptionalValidation<ledger.Transaction> {
    return {
        commandId: native('string'),
        workflowId: native('string'),
    };
}

export const Transaction: Validation = object<ledger.Transaction>('Transaction', required, optional);