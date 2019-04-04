// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface LedgerConfiguration
 * @memberof ledger
 */
export interface LedgerConfiguration {
    /**
     * @member {ledger.Duration} maxTtl
     * @memberof ledger.LedgerConfiguration
     * @instance
     */
    maxTtl: ledger.Duration
    /**
     * @member {ledger.Duration} minTtl
     * @memberof ledger.LedgerConfiguration
     * @instance
     */
    minTtl: ledger.Duration
}