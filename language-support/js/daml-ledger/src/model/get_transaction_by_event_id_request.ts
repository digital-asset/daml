// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @interface GetTransactionByEventIdRequest
 * @memberof ledger
 */
export interface GetTransactionByEventIdRequest {
    /**
     * @member {string} eventId
     * @memberof ledger.GetTransactionByEventIdRequest
     * @instance
     */
    eventId: string
    /**
     * @member {Array<string>} requestingParties
     * @memberof ledger.GetTransactionByEventIdRequest
     * @instance
     */
    requestingParties: string[]
}