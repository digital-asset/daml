// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface CompletionStreamRequest
 * @memberof ledger
 */
export interface CompletionStreamRequest {
    /**
     * @member {string} applicationId
     * @memberof ledger.CompletionStreamRequest
     * @instance
     */
    applicationId: string
    /**
     * @member {ledger.LedgerOffset} offset
     * @memberof ledger.CompletionStreamRequest
     * @instance
     */
    offset: ledger.LedgerOffset
    /**
     * @member {Array<string>} parties
     * @memberof ledger.CompletionStreamRequest
     * @instance
     */
    parties: string[]
}