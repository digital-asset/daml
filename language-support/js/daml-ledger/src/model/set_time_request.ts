// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface SetTimeRequest
 * @memberof ledger
 */
export interface SetTimeRequest {
    /**
     * @member {ledger.Timestamp} currentTime
     * @memberof ledger.SetTimeRequest
     * @instance
     */
    currentTime: ledger.Timestamp
    /**
     * @member {ledger.Timestamp} newTime
     * @memberof ledger.SetTimeRequest
     * @instance
     */
    newTime: ledger.Timestamp
}