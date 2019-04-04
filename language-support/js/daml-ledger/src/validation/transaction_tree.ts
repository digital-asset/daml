// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation, array } from "./base";
import { RequiredValidation, OptionalValidation } from "./base/typelevel";
import { Timestamp } from "./timestamp";
import { TreeEvent } from "./tree_event";
import { record } from "./base/record";

function required(): RequiredValidation<ledger.TransactionTree> {
    return {
        effectiveAt: Timestamp,
        eventsById: record(TreeEvent),
        rootEventIds: array(native('string')),
        offset: native('string'),
        transactionId: native('string')
    };
}

function optional(): OptionalValidation<ledger.TransactionTree> {
    return {
        commandId: native('string'),
        workflowId: native('string'),
    };
}

export const TransactionTree: Validation = object<ledger.TransactionTree>('TransactionTree', required, optional);