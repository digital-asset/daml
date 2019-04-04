// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.Duration> {
    return {
        nanoseconds: native('number'),
        seconds: native('number')
    };
}

export const Duration: Validation = object<ledger.Duration>('Duration', required, () => ({}));