// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { union, Validation } from "./base";
import { CreateCommand } from "./create_command";
import { ExerciseCommand } from "./exercise_command";

function values(): Record<keyof ledger.Command, Validation> {
    return {
        create: CreateCommand,
        exercise: ExerciseCommand
    };
}

export const Command: Validation = union('Command', values);