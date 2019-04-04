// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface InclusiveFilters
 * @memberof ledger
 */
export interface InclusiveFilters {
    /**
     * @member {Array<ledger.Identifier>} templateIds
     * @memberof ledger.InclusiveFilters
     * @instance
     */
    templateIds: ledger.Identifier[]
}