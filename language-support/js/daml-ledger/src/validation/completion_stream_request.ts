// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, array, native } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { LedgerOffset } from "./ledger_offset";

function required(): RequiredValidation<ledger.CompletionStreamRequest> {
    return {
        applicationId: native('string'),
        offset: LedgerOffset,
        parties: array(native('string')),
    };
}

export const CompletionStreamRequest: Validation = object<ledger.CompletionStreamRequest>('CompletionStreamRequest', required, () => ({}));