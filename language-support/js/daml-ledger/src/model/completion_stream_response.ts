// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface CompletionStreamResponse
 * @memberof ledger
 */
export interface CompletionStreamResponse {
    /**
     * @member {ledger.Checkpoint} checkpoint
     * @memberof ledger.CompletionStreamResponse
     * @instance
     */
    checkpoint?: ledger.Checkpoint
    /**
     * @member {Array<ledger.Completion>} completions
     * @memberof ledger.CompletionStreamResponse
     * @instance
     */
    completions?: ledger.Completion[]
}