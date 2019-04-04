// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const CreateCommand: mapping.Mapping<grpc.CreateCommand, ledger.CreateCommand> = {
    toObject(command: grpc.CreateCommand): ledger.CreateCommand {
        return {
            templateId: mapping.Identifier.toObject(command.getTemplateId()!),
            arguments:  mapping.Record.toObject(command.getCreateArguments()!)    
        }
    },
    toMessage(command: ledger.CreateCommand): grpc.CreateCommand {
        const result = new grpc.CreateCommand();
        result.setTemplateId(mapping.Identifier.toMessage(command.templateId));
        result.setCreateArguments(mapping.Record.toMessage(command.arguments));
        return result;
    }
}