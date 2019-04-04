// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetTimeResponse
 * @memberof ledger
 */
export interface GetTimeResponse {
    /**
     * @member {ledger.Timestamp} currentTime
     * @memberof ledger.GetTimeResponse
     * @instance
     */
    currentTime: ledger.Timestamp
}