// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetTransactionsRequest
 * @memberof ledger
 */
export interface GetTransactionsRequest {
    /**
     * @member {ledger.LedgerOffset} begin
     * @memberof ledger.GetTransactionsRequest
     * @instance
     */
    begin: ledger.LedgerOffset
    /**
     * @member {ledger.LedgerOffset} end
     * @memberof ledger.GetTransactionsRequest
     * @instance
     */
    end?: ledger.LedgerOffset
    /**
     * @member {ledger.TransactionFilter} filter
     * @memberof ledger.GetTransactionsRequest
     * @instance
     */
    filter: ledger.TransactionFilter
    /**
     * @member {boolean} verbose
     * @memberof ledger.GetTransactionsRequest
     * @instance
     */
    verbose?: boolean
}