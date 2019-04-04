// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface ExerciseCommand
 * @memberof ledger
 */
export interface ExerciseCommand {
    /**
     * @member {ledger.Identifier} templateId
     * @memberof ledger.ExerciseCommand
     * @instance
     */
    templateId: ledger.Identifier
    /**
     * @member {string} choice
     * @memberof ledger.ExerciseCommand
     * @instance
     */
    choice: string
    /**
     * @member {string} contractId
     * @memberof ledger.ExerciseCommand
     * @instance
     */
    contractId: string
    /**
     * @member {ledger.Value} argument
     * @memberof ledger.ExerciseCommand
     * @instance
     */
    argument: ledger.Value
}