// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation, array } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Identifier } from "./identifier";

function required(): RequiredValidation<ledger.ArchivedEvent> {
    return {
        contractId: native('string'),
        eventId: native('string'),
        templateId: Identifier,
        witnessParties: array(native('string'))
    };
}

export const ArchivedEvent: Validation = object<ledger.ArchivedEvent>('ArchivedEvent', required, () => ({}));