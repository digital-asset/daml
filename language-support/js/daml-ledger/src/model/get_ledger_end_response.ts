// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetLedgerEndResponse
 * @memberof ledger
 */
export interface GetLedgerEndResponse {
    /**
     * @member {ledger.LedgerOffset} offset
     * @memberof ledger.GetLedgerEndResponse
     * @instance
     */
    offset: ledger.LedgerOffset
}