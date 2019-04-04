// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface TransactionTree
 * @memberof ledger
 */
export interface TransactionTree {
    /**
     * @member {string} commandId
     * @memberof ledger.TransactionTree
     * @instance
     */
    commandId?: string
    /**
     * @member {ledger.Timestamp} effectiveAt
     * @memberof ledger.TransactionTree
     * @instance
     */
    effectiveAt: ledger.Timestamp
    /**
     * @member {string} offset
     * @memberof ledger.TransactionTree
     * @instance
     */
    offset: string
    /**
     * @member {string} transactionId
     * @memberof ledger.TransactionTree
     * @instance
     */
    transactionId: string
    /**
     * @member {string} workflowId
     * @memberof ledger.TransactionTree
     * @instance
     */
    workflowId?: string
    /**
     * @member {object} eventsByIds
     * @memberof ledger.TransactionTree
     * @instance
     */
    eventsById: Record<string, ledger.TreeEvent>
    /**
     * @member {Array<string>} rootEventIds
     * @memberof ledger.TransactionTree
     * @instance
     */
    rootEventIds: string[]
}