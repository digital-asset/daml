// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface CreatedEvent
 * @memberof ledger
 */
export interface CreatedEvent {
    /**
     * @member {string} eventId
     * @memberof ledger.CreatedEvent
     * @instance
     */
    eventId: string
    /**
     * @member {string} contractId
     * @memberof ledger.CreatedEvent
     * @instance
     */
    contractId: string
    /**
     * @member {ledger.Identifier} templateId
     * @memberof ledger.CreatedEvent
     * @instance
     */
    templateId: ledger.Identifier
    /**
     * @member {ledger.Record} arguments
     * @memberof ledger.CreatedEvent
     * @instance
     */
    arguments: ledger.Record
    /**
     * @member {Array<string>} witnessParties
     * @memberof ledger.CreatedEvent
     * @instance
     */
    witnessParties: string[]
}