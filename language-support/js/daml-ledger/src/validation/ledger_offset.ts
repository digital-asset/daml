// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { enumeration, native, union, Validation } from "./base";

function values(): Record<keyof ledger.LedgerOffset, Validation> {
    return {
        absolute: native('string'),
        boundary: enumeration(ledger.LedgerOffset.Boundary, 'LedgerOffset.Boundary'),
    };
}

export const LedgerOffset: Validation = union('LedgerOffset', values);