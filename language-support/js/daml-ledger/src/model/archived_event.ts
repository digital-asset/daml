// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ArchivedEvent
 * @memberof ledger
 */
export interface ArchivedEvent {
    /**
     * @member {string} contractId
     * @memberof ledger.ArchivedEvent
     * @instance
     */
    contractId: string
    /**
     * @member {string} eventId
     * @memberof ledger.ArchivedEvent
     * @instance
     */
    eventId: string
    /**
     * @member {ledger.Identifier} templateId
     * @memberof ledger.ArchivedEvent
     * @instance
     */
    templateId: ledger.Identifier
    /**
     * @member {Array<string>} witnessParties
     * @memberof ledger.ArchivedEvent
     * @instance
     */
    witnessParties: string[]
}