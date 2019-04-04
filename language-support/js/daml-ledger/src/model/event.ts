// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Event
 * @memberof ledger
 */
export interface Event {
    /**
     * @member {ledger.ArchivedEvent} archived
     * @memberof ledger.Event
     * @instance
     */
    archived?: ledger.ArchivedEvent
    /**
     * @member {ledger.CreatedEvent} created
     * @memberof ledger.Event
     * @instance
     */
    created?: ledger.CreatedEvent
    /**
     * @member {ledger.ExercisedEvent} exercised
     * @memberof ledger.Event
     * @instance
     */
    exercised?: ledger.ExercisedEvent
}