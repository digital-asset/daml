// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @interface Timestamp
 * @memberof ledger
 */
export interface Timestamp {
    /**
     * @member {number} seconds
     * @memberof ledger.Timestamp
     * @instance
     */
    seconds: number
    /**
     * @member {number} nanoseconds
     * @memberof ledger.Timestamp
     * @instance
     */
    nanoseconds: number
}

export namespace Timestamp {

    /**
     * True if `left` represents an instant that precedes or is simulatenous to `right`
     *
     * @function lessThanOrEqual
     * @memberof ledger.Timestamp
     * @param {ledger.Timestamp} left
     * @param {ledger.Timestamp} right
     */
    export function lessThanOrEqual(left: Timestamp, right: Timestamp): boolean {
        if (left.seconds < right.seconds) {
            return true;
        } else if (left.seconds === right.seconds) {
            return left.nanoseconds <= right.nanoseconds;
        } else {
            return false;
        }
    }

}