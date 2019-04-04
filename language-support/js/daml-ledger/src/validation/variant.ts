// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation } from "./base";
import { Identifier } from "./identifier";
import { Value } from './value'
import { RequiredValidation, OptionalValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.Variant> {
    return {
        constructor: native('string'),
        value: Value,
    };
}

function optional(): OptionalValidation<ledger.Variant> {
    return {
        variantId: Identifier,
    };
}

export const Variant: Validation = object('Variant', required, optional);