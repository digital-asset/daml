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
     *
     * Represented as a {string} to avoid losing precision
     */
    decimal?: string
    /**
     * @member {string} text
     * @memberof ledger.Value
     * @instance
     */
    text?: string
    /**
     * @member {string} timestamp
     * @memberof ledger.Value
     * @instance
     *
     * Represented as a {string} to avoid losing precision
     */
    timestamp?: string
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
     * @member {string} date
     * @memberof ledger.Value
     * @instance
     *
     * Represented as a {string} for consistency with
     * other numeric types in this union. This also
     * allows the type to remain stable in the face
     * of prospective expansions of the underlying
     * type to a 64-bit encoding.
     */
    date?: string
    /**
     * @member {ledger.Optional} optional
     * @memberof ledger.Value
     * @instance
     */
    optional?: ledger.Optional
}