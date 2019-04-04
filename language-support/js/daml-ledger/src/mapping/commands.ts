// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Commands: mapping.Mapping<grpc.Commands, ledger.Commands> = {
    toObject(commands: grpc.Commands): ledger.Commands {
        const result: ledger.Commands = {
            applicationId: commands.getApplicationId(),
            commandId: commands.getCommandId(),
            party: commands.getParty(),
            ledgerEffectiveTime: mapping.Timestamp.toObject(commands.getLedgerEffectiveTime()!),
            maximumRecordTime: mapping.Timestamp.toObject(commands.getMaximumRecordTime()!),
            list: commands.getCommandsList().map((command) => mapping.Command.toObject(command))
        };
        const workflowId = commands.getWorkflowId();
        if (workflowId !== undefined && workflowId !== '') {
            result.workflowId = workflowId;
        }
        return result;
    },
    toMessage(commands: ledger.Commands): grpc.Commands {
        const result = new grpc.Commands();
        result.setCommandId(commands.commandId);
        result.setParty(commands.party);
        result.setLedgerEffectiveTime(mapping.Timestamp.toMessage(commands.ledgerEffectiveTime));
        result.setMaximumRecordTime(mapping.Timestamp.toMessage(commands.maximumRecordTime));
        result.setCommandsList(commands.list.map(mapping.Command.toMessage));
        if (commands.workflowId) {
            result.setWorkflowId(commands.workflowId);
        }
        if (commands.applicationId) {
            result.setApplicationId(commands.applicationId);
        }
        return result;
    }
}