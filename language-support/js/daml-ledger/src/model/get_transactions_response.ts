// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetTransactionsResponse
 * @memberof ledger
 */
export interface GetTransactionsResponse {
    /**
     * @member {Array<ledger.Transaction>} transactions
     * @memberof ledger.GetTransactionsResponse
     * @instance
     */
    transactions: ledger.Transaction[]
}