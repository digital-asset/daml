// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Validation, object } from "./base";
import * as ledger from "..";
import { InclusiveFilters } from './inclusive_filters';
import { OptionalValidation } from "./base/typelevel";

function optional(): OptionalValidation<ledger.Filters> {
    return {
        inclusive: InclusiveFilters
    };
}

export const Filters: Validation = object<ledger.Filters>('Filters', () => ({}), optional);