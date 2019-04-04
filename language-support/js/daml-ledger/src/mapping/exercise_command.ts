// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const ExerciseCommand: mapping.Mapping<grpc.ExerciseCommand, ledger.ExerciseCommand> = {
    toObject(command: grpc.ExerciseCommand): ledger.ExerciseCommand {
        return {
            templateId: mapping.Identifier.toObject(command.getTemplateId()!),
            contractId: command.getContractId(),
            choice: command.getChoice(),
            argument: mapping.Value.toObject(command.getChoiceArgument()!),
        };
    },
    toMessage(command: ledger.ExerciseCommand): grpc.ExerciseCommand {
        const result = new grpc.ExerciseCommand();
        result.setTemplateId(mapping.Identifier.toMessage(command.templateId));
        result.setContractId(command.contractId);
        result.setChoice(command.choice);
        result.setChoiceArgument(mapping.Value.toMessage(command.argument));
        return result;
    }
}