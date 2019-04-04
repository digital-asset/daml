// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { LedgerOffset } from "./ledger_offset";
import { CreatedEvent } from "./created_event";

/**
 * @interface GetActiveContractsResponse
 * @memberof ledger
 */
export interface GetActiveContractsResponse {
    /**
     * @member {ledger.LedgerOffset} offset
     * @memberof ledger.GetActiveContractsResponse
     * @instance
     */
    offset: LedgerOffset
    /**
     * @member {string} workflowId
     * @memberof ledger.GetActiveContractsResponse
     * @instance
     */
    workflowId?: string
    /**
     * @member {Array<ledger.CreatedEvent>} activeContracts
     * @memberof ledger.GetActiveContractsResponse
     * @instance
     */
    activeContracts?: CreatedEvent[]
}