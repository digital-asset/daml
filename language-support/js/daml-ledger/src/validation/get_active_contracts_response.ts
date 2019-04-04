// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, native, object, Validation } from "./base";
import { OptionalValidation, RequiredValidation } from "./base/typelevel";
import { CreatedEvent } from "./created_event";
import { LedgerOffset } from "./ledger_offset";

function required(): RequiredValidation<ledger.GetActiveContractsResponse> {
    return {
        offset: LedgerOffset
    };
}

function optional(): OptionalValidation<ledger.GetActiveContractsResponse> {
    return {
        activeContracts: array(CreatedEvent),
        workflowId: native('string')
    }
}

export const GetActiveContractsResponse: Validation = object<ledger.GetActiveContractsResponse>('GetActiveContractsResponse', required, optional);