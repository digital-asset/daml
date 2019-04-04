// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface CreateCommand
 * @memberof ledger
 */
export interface CreateCommand {
    /**
     * @member {ledger.Identifier} templateId
     * @memberof ledger.CreateCommand
     * @instance
     */
    templateId: ledger.Identifier
    /**
     * @member {ledger.Record} arguments
     * @memberof ledger.CreateCommand
     * @instance
     */
    arguments: ledger.Record
}