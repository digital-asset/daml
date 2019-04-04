// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Identifier } from "./identifier";
import { Record } from "./record";

function required(): RequiredValidation<ledger.CreateCommand> {
    return {
        arguments: Record,
        templateId: Identifier
    };
}

export const CreateCommand: Validation = object<ledger.CreateCommand>('CreateCommand', required, () => ({}));