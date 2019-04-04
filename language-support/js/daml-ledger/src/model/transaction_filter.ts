// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface TransactionFilter
 * @memberof ledger
 */
export interface TransactionFilter {
    /**
     * A plain object with {@link ledger.Filters} as values.
     *
     * @member {object} filtersByParty
     * @memberof ledger.TransactionFilter
     * @instance
     */
    filtersByParty: Record<string, ledger.Filters>
}