// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetActiveContractsRequest
 * @memberof ledger
 */
export interface GetActiveContractsRequest {
    /**
     * @member {ledger.TransactionFilter} filter
     * @memberof ledger.GetActiveContractsRequest
     * @instance
     */
    filter: ledger.TransactionFilter
    /**
     * @member {boolean} verbose
     * @memberof ledger.GetActiveContractsRequest
     * @instance
     */
    verbose?: boolean
}