// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface GetPackageStatusResponse
 * @memberof ledger
 */
export interface GetPackageStatusResponse {
    /**
     * @member {ledger.PackageStatus} status
     * @memberof ledger.GetPackageStatusResponse
     * @instance
     */
    status: ledger.PackageStatus
}