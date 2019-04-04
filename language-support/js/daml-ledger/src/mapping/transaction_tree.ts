// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const TransactionTree: mapping.Mapping<grpc.TransactionTree, ledger.TransactionTree> = {
    toObject(message: grpc.TransactionTree): ledger.TransactionTree {
        const eventsById: Record<string, ledger.TreeEvent> = {};
        message.getEventsByIdMap().forEach((event, id) => {
            eventsById[id] = mapping.TreeEvent.toObject(event);
        });
        const transactionTree: ledger.TransactionTree = {
            effectiveAt: mapping.Timestamp.toObject(message.getEffectiveAt()!),
            eventsById: eventsById,
            rootEventIds: [...message.getRootEventIdsList()],
            offset: message.getOffset(),
            transactionId: message.getTransactionId(),
        };
        const commandId = message.getCommandId();
        if (commandId !== undefined && commandId !== '') {
            transactionTree.commandId = commandId;
        }
        const workflowId = message.getWorkflowId();
        if (workflowId !== undefined && workflowId !== '') {
            transactionTree.workflowId = workflowId;
        }
        return transactionTree;
    },
    toMessage(object: ledger.TransactionTree): grpc.TransactionTree {
        const message = new grpc.TransactionTree();
        message.setEffectiveAt(mapping.Timestamp.toMessage(object.effectiveAt));
        for (const id in object.eventsById) {
            const event = mapping.TreeEvent.toMessage(object.eventsById[id]);
            message.getEventsByIdMap().set(id, event);
        }
        message.setRootEventIdsList([... object.rootEventIds]);
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