// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { Any } from "./any";
import { array, native, object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.Status> {
    return {
        code: native('number'),
        details: array(Any),
        message: native('string')
    };
}

export const Status: Validation = object<ledger.Status>('Status', required, () => ({}));