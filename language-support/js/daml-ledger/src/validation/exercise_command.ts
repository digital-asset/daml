// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, native } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Identifier } from "./identifier";
import { Value } from "./value";

function required(): RequiredValidation<ledger.ExerciseCommand> {
    return {
        argument: Value,
        choice: native('string'),
        contractId: native('string'),
        templateId: Identifier
    };
}

export const ExerciseCommand: Validation = object<ledger.ExerciseCommand>('ExerciseCommand', required, () => ({}));