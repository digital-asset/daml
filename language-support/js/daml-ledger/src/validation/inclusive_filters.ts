// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, object, Validation } from "./base";
import { RequiredValidation } from './base/typelevel';
import { Identifier } from './identifier';

function required(): RequiredValidation<ledger.InclusiveFilters> {
    return {
        templateIds: array(Identifier),
    };
}

export const InclusiveFilters: Validation = object<ledger.InclusiveFilters>('InclusiveFilters', required, () => ({}));