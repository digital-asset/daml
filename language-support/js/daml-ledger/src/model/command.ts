// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '..';

/**
 * @interface Command
 * @memberof ledger
 */ 
export interface Command {
    /**
     * @member {ledger.CreateCommand} create
     * @memberof ledger.Command
     * @instance
     */
    create?: ledger.CreateCommand
    /**
     * @member {ledger.ExerciseCommand} exercise
     * @memberof ledger.Command
     * @instance
     */
    exercise?: ledger.ExerciseCommand
}