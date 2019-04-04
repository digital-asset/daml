// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ExercisedEvent
 * @memberof ledger
 */
export interface ExercisedEvent {
    /**
     * @member {Array<string>} actingParties
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    actingParties: string[]
    /**
     * @member {Array<string>} childEventIds
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    childEventIds?: string[]
    /**
     * @member {string} choice
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    choice: string
    /**
     * @member {ledger.Value} argument
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    argument: ledger.Value
    /**
     * @member {boolean} consuming
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    consuming: boolean
    /**
     * @member {string} contractCreatingEventId
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    contractCreatingEventId: string
    /**
     * @member {string} contractId
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    contractId: string
    /**
     * @member {string} eventId
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    eventId: string
    /**
     * @member {ledger.Identifier} templateId
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    templateId: ledger.Identifier
    /**
     * @member {Array<string>} witnessParties
     * @memberof ledger.ExercisedEvent
     * @instance
     */
    witnessParties: string[]
}