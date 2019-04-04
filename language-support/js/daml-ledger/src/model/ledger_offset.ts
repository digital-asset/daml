// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @interface LedgerOffset
 * @memberof ledger
 */ 
export interface LedgerOffset {
    /**
     * @member {ledger.LedgerOffset.Boundary} boundary
     * @memberof ledger.LedgerOffset
     * @instance
     */
    boundary?: LedgerOffset.Boundary
    /**
     * @member {ledger.LedgerOffset.Absolute} absolute
     * @memberof ledger.LedgerOffset
     * @instance
     */
    absolute?: LedgerOffset.Absolute
}

export namespace LedgerOffset {

    /**
     * An enumeration of possible boundaries of a ledger.
     *
     * @interface Boundary
     * @memberof ledger.LedgerOffset
     */
    export enum Boundary {
        /**
         * @member BEGIN
         * @memberof ledger.LedgerOffset.Boundary
         */
        BEGIN,
        /**
         * @member END
         * @memberof ledger.LedgerOffset.Boundary
         */
        END
    }

    /**
     * @typedef {string} Absolute
     * @memberof ledger.LedgerOffset
     */
    export type Absolute = string

}