// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Transaction: mapping.Mapping<grpc.Transaction, ledger.Transaction> = {
    toObject(message: grpc.Transaction): ledger.Transaction {
        const transaction: ledger.Transaction = {
            effectiveAt: mapping.Timestamp.toObject(message.getEffectiveAt()!),
            events: message.getEventsList().map((e: grpc.Event) => mapping.Event.toObject(e)),
            offset: message.getOffset(),
            transactionId: message.getTransactionId(),
        };
        const commandId = message.getCommandId();
        if (commandId !== undefined && commandId !== '') {
            transaction.commandId = commandId;
        }
        const workflowId = message.getWorkflowId();
        if (workflowId !== undefined && workflowId !== '') {
            transaction.workflowId = workflowId;
        }
        return transaction;
    },
    toMessage(object: ledger.Transaction): grpc.Transaction {
        const message = new grpc.Transaction();
        message.setEffectiveAt(mapping.Timestamp.toMessage(object.effectiveAt));
        message.setEventsList(object.events.map((e: ledger.Event) => mapping.Event.toMessage(e)));
        message.setOffset(object.offset);
        message.setTransactionId(object.transactionId);
        if (object.commandId) {
            message.setCommandId(object.commandId);
        }
        if (object.workflowId) {
            message.setWorkflowId(object.workflowId);
        }
        return message;
    }
}