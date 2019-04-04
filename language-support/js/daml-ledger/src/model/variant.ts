// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Variant
 * @memberof ledger
 */
export interface Variant {
    /**
     * @member {ledger.Identifier} variantId
     * @memberof ledger.Variant
     * @instance
     */
    variantId?: ledger.Identifier
    /**
     * @member {string} constructor
     * @memberof ledger.Variant
     * @instance
     */
    constructor: string
    /**
     * @member {ledger.Value} value
     * @memberof ledger.Variant
     * @instance
     */
    value: ledger.Value
}