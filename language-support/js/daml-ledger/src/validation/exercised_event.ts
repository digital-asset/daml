// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, native, object, Validation } from "./base";
import { OptionalValidation, RequiredValidation } from "./base/typelevel";
import { Identifier } from "./identifier";
import { Value } from "./value";

function required(): RequiredValidation<ledger.ExercisedEvent> {
    return {
        actingParties: array(native('string')),
        choice: native('string'),
        argument: Value,
        consuming: native('boolean'),
        contractCreatingEventId: native('string'),
        contractId: native('string'),
        eventId: native('string'),
        templateId: Identifier,
        witnessParties: array(native('string'))
    };
}

function optional(): OptionalValidation<ledger.ExercisedEvent> {
    return {
        childEventIds: array(native('string'))
    };
}

export const ExercisedEvent: Validation = object<ledger.ExercisedEvent>('ExercisedEvent', required, optional);