// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @interface Identifier
 * @memberof ledger
 */
export interface Identifier {
    /**
     * @member {string} packageId
     * @memberof ledger.Identifier
     * @instance
     */
    packageId: string
    /**
     * @member {string} moduleName
     * @memberof ledger.Identifier
     * @instance
     */
    moduleName: string
    /**
     * @member {string} entityName
     * @memberof ledger.Identifier
     * @instance
     */
    entityName: string
}