// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Completion
 * @memberof ledger
 */
export interface Completion {
    /**
     * @member {string} commandId
     * @memberof ledger.Completion
     * @instance
     */
    commandId: string
    /**
     * @member {Status} status
     * @memberof ledger.Completion
     * @instance
     */
    status?: ledger.Status
}