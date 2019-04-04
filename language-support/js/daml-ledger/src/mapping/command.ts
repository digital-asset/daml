// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

import { inspect } from 'util';

export const Command: mapping.Mapping<grpc.Command, ledger.Command> = {
    toObject(command: grpc.Command): ledger.Command {
        if (command.hasCreate()) {
            return { create: mapping.CreateCommand.toObject(command.getCreate()!) };
        } else if (command.hasExercise()) {
            return { exercise: mapping.ExerciseCommand.toObject(command.getExercise()!) };
        } else {
            throw new Error(`Expected either CreateCommand or ExerciseCommand, found ${inspect(command)}`);
        }
    },
    toMessage(command: ledger.Command): grpc.Command {
        const result = new grpc.Command();
        if (command.create !== undefined) {
            result.setCreate(mapping.CreateCommand.toMessage(command.create));
        } else if (command.exercise !== undefined) {
            result.setExercise(mapping.ExerciseCommand.toMessage(command.exercise));
        } else {
            throw new Error(`Expected either LedgerOffset Absolute or LedgerOffset Boundary, found ${inspect(command)}`);
        }
        return result;
    }
}