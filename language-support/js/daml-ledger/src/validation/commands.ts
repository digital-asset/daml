// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, native, object, Validation } from "./base";
import { OptionalValidation, RequiredValidation } from "./base/typelevel";
import { Command } from "./command";
import { Timestamp } from "./timestamp";

function required(): RequiredValidation<ledger.Commands> {
    return {
        applicationId: native('string'),
        commandId: native('string'),
        ledgerEffectiveTime: Timestamp,
        list: array(Command),
        maximumRecordTime: Timestamp,
        party: native('string')
    };
}

function optional(): OptionalValidation<ledger.Commands> {
    return {
        workflowId: native('string')
    };
}

export const Commands: Validation = object<ledger.Commands>('Commands', required, optional);