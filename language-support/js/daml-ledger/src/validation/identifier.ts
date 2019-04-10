// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.Identifier> {
    return {
        packageId: native('string'),
        moduleName: native('string'),
        entityName: native('string')
    };
}

export const Identifier: Validation = object<ledger.Identifier>('Identifier', required, () => ({}));