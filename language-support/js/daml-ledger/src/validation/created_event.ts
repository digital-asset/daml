// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, native, array } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Identifier } from "./identifier";
import { Record } from "./record";

function required(): RequiredValidation<ledger.CreatedEvent> {
    return {
        arguments: Record,
        contractId: native('string'),
        eventId: native('string'),
        templateId: Identifier,
        witnessParties: array(native('string')),
    };
}

export const CreatedEvent: Validation = object<ledger.CreatedEvent>('CreatedEvent', required, () => ({}));