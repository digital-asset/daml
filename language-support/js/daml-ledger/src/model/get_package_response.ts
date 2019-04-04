// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetPackageResponse
 * @memberof ledger
 */
export interface GetPackageResponse {
    /**
     * @member {ledger.HashFunction} hashFunction
     * @memberof ledger.GetPackageResponse
     * @instance
     */
    hashFunction: ledger.HashFunction
    /**
     * @member {string} hash
     * @memberof ledger.GetPackageResponse
     * @instance
     */
    hash: string
    /**
     * @member {string} payload
     * @memberof ledger.GetPackageResponse
     * @instance
     */
    archivePayload: string
}