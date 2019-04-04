// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Record
 * @memberof ledger
 */
export interface Record {
    /**
     * @member {ledger.Identifier} recordId
     * @memberof ledger.Record
     * @instance
     */
    recordId?: ledger.Identifier
    /**
     * A plain object with {@link ledger.Value} values.
     *
     * @member {object} fields
     * @memberof ledger.Record
     * @instance
     */
    fields: { [k: string]: ledger.Value }
}