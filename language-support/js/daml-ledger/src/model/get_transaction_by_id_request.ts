// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @interface GetTransactionByIdRequest
 * @memberof ledger
 */
export interface GetTransactionByIdRequest {
    /**
     * @member {string} transactionId
     * @memberof ledger.GetTransactionByIdRequest
     * @instance
     */
    transactionId: string
    /**
     * @member {Array<string>} requestingParties
     * @memberof ledger.GetTransactionByIdRequest
     * @instance
     */
    requestingParties: string[]
}