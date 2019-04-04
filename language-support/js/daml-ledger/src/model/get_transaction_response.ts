// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetTransactionResponse
 * @memberof ledger
 */
export interface GetTransactionResponse {
    /**
     * @member {ledger.TransactionTree} transaction
     * @memberof ledger.GetTransactionResponse
     * @instance
     */
    transaction: ledger.TransactionTree
}