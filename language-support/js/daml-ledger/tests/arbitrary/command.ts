// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { CreateCommand } from './create_command';
import { ExerciseCommand } from './exercise_command';

const Create: jsc.Arbitrary<ledger.Command> =
    CreateCommand.smap<{ create: ledger.CreateCommand }>(
        create => ({ create: create }),
        command => command.create
    );
const Exercise: jsc.Arbitrary<ledger.Command> =
    ExerciseCommand.smap<{ exercise: ledger.ExerciseCommand }>(
        exercise => ({ exercise: exercise }),
        command => command.exercise
    );

export const Command: jsc.Arbitrary<ledger.Command> = jsc.oneof([Create, Exercise]);