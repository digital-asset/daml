// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { LedgerOffset } from "./ledger_offset";
import { Timestamp } from "./timestamp";

function required(): RequiredValidation<ledger.Checkpoint> {
    return {
        offset: LedgerOffset,
        recordTime: Timestamp
    };
}

export const Checkpoint: Validation = object<ledger.Checkpoint>('Checkpoint', required, () => ({}));