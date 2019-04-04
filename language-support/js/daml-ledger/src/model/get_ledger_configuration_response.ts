// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetLedgerConfigurationResponse
 * @memberof ledger
 */
export interface GetLedgerConfigurationResponse {
    /**
     * @member {ledger.LedgerConfiguration} config
     * @memberof GetLedgerConfigurationResponse
     * @instance
     */
    config: ledger.LedgerConfiguration
}