// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Commands
 * @memberof ledger
 */
export interface Commands {
    /**
     * @member {string} applicationId
     * @memberof ledger.Commands
     * @instance
     */
    applicationId: string
    /**
     * @member {string} commandId
     * @memberof ledger.Commands
     * @instance
     */
    commandId: string
    /**
     * @member {string} party
     * @memberof ledger.Commands
     * @instance
     */
    party: string
    /**
     * @member {string} workflowId
     * @memberof ledger.Commands
     * @instance
     */
    workflowId?: string
    /**
     * @member {ledger.Timestamp} ledgerEffectiveTime
     * @memberof ledger.Commands
     * @instance
     */
    ledgerEffectiveTime: ledger.Timestamp
    /**
     * @member {ledger.Timestamp} maximumRecordTime
     * @memberof ledger.Commands
     * @instance
     */
    maximumRecordTime: ledger.Timestamp
    /**
     * @member {Array<Command>} list
     * @memberof ledger.Commands
     * @instance
     */
    list: ledger.Command[]
}