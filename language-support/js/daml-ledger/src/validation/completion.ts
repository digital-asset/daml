// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation } from "./base";
import { OptionalValidation, RequiredValidation } from "./base/typelevel";
import { Status } from "./status";

function required(): RequiredValidation<ledger.Completion> {
    return {
        commandId: native('string'),
    };
}

function optional(): OptionalValidation<ledger.Completion> {
    return {
        status: Status
    };
}

export const Completion: Validation = object<ledger.Completion>('Completion', required, optional);