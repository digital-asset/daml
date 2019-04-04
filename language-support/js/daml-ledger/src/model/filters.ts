// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Filters
 * @memberof ledger
 */
export interface Filters {
    /**
     * @member {ledger.InclusiveFilters} inclusive
     * @memberof ledger.Filters
     * @instance
     */
    inclusive?: ledger.InclusiveFilters
}