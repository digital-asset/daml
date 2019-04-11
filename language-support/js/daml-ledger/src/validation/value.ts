// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, native, union, Validation } from "./base";
import { Empty } from "./empty";
import { Optional } from "./optional";
import { Record } from "./record";
import { Timestamp } from "./timestamp";
import { Variant } from "./variant";

function values(): Record<keyof ledger.Value, Validation> {
    return {
        bool: native('boolean'),
        contractId: native('string'),
        date: native('string'),
        decimal: native('string'),
        int64: native('string'),
        list: array(Value),
        party: native('string'),
        record: Record,
        text: native('string'),
        timestamp: native('string'),
        unit: Empty,
        variant: Variant,
        optional: Optional
    };
}

export const Value: Validation = union('Value', values);