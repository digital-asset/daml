// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Transaction
 * @memberof ledger
 */
export interface Transaction {
    /**
     * @member {string} commandId
     * @memberof ledger.Transaction
     * @instance
     */
    commandId?: string
    /**
     * @member {ledger.Timestamp} effectiveAt
     * @memberof ledger.Transaction
     * @instance
     */
    effectiveAt: ledger.Timestamp
    /**
     * @member {Array<ledger.Event>} events
     * @memberof ledger.Transaction
     * @instance
     */
    events: ledger.Event[]
    /**
     * @member {string} offset
     * @memberof ledger.Transaction
     * @instance
     */
    offset: string
    /**
     * @member {string} transactionId
     * @memberof ledger.Transaction
     * @instance
     */
    transactionId: string
    /**
     * @member {string} workflowId
     * @memberof ledger.Transaction
     * @instance
     */
    workflowId?: string
}