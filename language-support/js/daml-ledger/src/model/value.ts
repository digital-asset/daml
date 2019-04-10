// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ledger.Value
 */
export interface Value {
    /**
     * @member {ledger.Record} record
     * @memberof ledger.Value
     * @instance
     */
    record?: ledger.Record
    /**
     * @member {ledger.Variant} variant
     * @memberof ledger.Value
     * @instance
     */
    variant?: ledger.Variant
    /**
     * @member {string} contractId
     * @memberof ledger.Value
     * @instance
     */
    contractId?: string
    /**
     * @member {Array<ledger.Value>} list
     * @memberof ledger.Value
     * @instance
     */
    list?: Value[]
    /**
     * @member {string} int64
     * @memberof ledger.Value
     * @instance
     *
     * Represented as a {string} to avoid losing precision
     */
    int64?: string
    /**
     * @member {string} decimal
     * @memberof ledger.Value
     * @instance
     */
    decimal?: string
    /**
     * @member {string} text
     * @memberof ledger.Value
     * @instance
     */
    text?: string
    /**
     * @member {number} timestamp
     * @memberof ledger.Value
     * @instance
     */
    timestamp?: number
    /**
     * @member {string} party
     * @memberof ledger.Value
     * @instance
     */
    party?: string
    /**
     * @member {boolean} bool
     * @memberof ledger.Value
     * @instance
     */
    bool?: boolean
    /**
     * @member {ledger.Empty} unit
     * @memberof ledger.Value
     * @instance
     */
    unit?: ledger.Empty
    /**
     * @member {number} date
     * @memberof ledger.Value
     * @instance
     */
    date?: number
    /**
     * @member {ledger.Optional} optional
     * @memberof ledger.Value
     * @instance
     */
    optional?: ledger.Optional
}