// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Checkpoint
 * @memberof ledger
 */
export interface Checkpoint {
    /**
     * @member {ledger.Timestamp} recordTime
     * @memberof ledger.Checkpoint
     * @instance
     */
    recordTime: ledger.Timestamp
    /**
     * @member {ledger.LedgerOffset} offset
     * @memberof ledger.Checkpoint
     * @instance
     */
    offset: ledger.LedgerOffset
}