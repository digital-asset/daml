// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetTransactionTreesResponse
 * @memberof ledger
 */
export interface GetTransactionTreesResponse {
    /**
     * @member {Array<ledger.Transaction>} transactions
     * @memberof ledger.GetTransactionTreesResponse
     * @instance
     */
    transactions: ledger.TransactionTree[]
}