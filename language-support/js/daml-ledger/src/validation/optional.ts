// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { OptionalValidation } from "./base/typelevel";
import { Value } from "./value";

function optional(): OptionalValidation<ledger.Optional> {
    return {
        value: Value
    };
}

export const Optional: Validation = object<ledger.Optional>('Optional', () => ({}), optional);