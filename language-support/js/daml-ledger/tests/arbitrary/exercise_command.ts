// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Value } from './record_value_variant';
import { Identifier } from './identifier';

export const ExerciseCommand: jsc.Arbitrary<ledger.ExerciseCommand> =
    jsc.tuple([Value, jsc.string, jsc.string, Identifier]).smap<ledger.ExerciseCommand>(
        ([args, choice, contractId, templateId]) => ({
            argument: args,
            choice: choice,
            contractId: contractId,
            templateId: templateId
        }),
        (exerciseCommand) =>
            [exerciseCommand.argument, exerciseCommand.choice, exerciseCommand.contractId, exerciseCommand.templateId]
    );