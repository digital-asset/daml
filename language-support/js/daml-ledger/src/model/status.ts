// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ledger.Status
 */
export interface Status {
    /**
     * @member {number} code
     * @memberof ledger.Status
     * @instance
     */
    code: number
    /**
     * @member {string} message
     * @memberof ledger.Status
     * @instance
     */
    message: string
    /**
     * @member {Array<ledger.Any>} details
     * @memberof ledger.Status
     * @instance
     */
    details: ledger.Any[]
}