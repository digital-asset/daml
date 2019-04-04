// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { record } from "./base/record";
import { OptionalValidation, RequiredValidation } from "./base/typelevel";
import { Identifier } from "./identifier";
import { Value } from './value';

function required(): RequiredValidation<ledger.Record> {
    return {
        fields: record(Value)
    };
}

function optional(): OptionalValidation<ledger.Record> {
    return {
        recordId: Identifier
    };
}

export const Record: Validation = object<ledger.Record>('Record', required, optional);