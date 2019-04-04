// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Timestamp } from './timestamp';
import { TreeEvent } from './event';
import { maybe } from './maybe';

export const TransactionTree: jsc.Arbitrary<ledger.TransactionTree> =
    jsc.tuple([maybe(jsc.string), Timestamp, jsc.dict(TreeEvent), jsc.array(jsc.string), jsc.string, jsc.string, maybe(jsc.string)]).smap<ledger.TransactionTree>(
        ([commandId, effectiveAt, eventsById, rootEventsId, offset, transactionId, workflowId]) => {
            const transactionTree: ledger.TransactionTree = {
                effectiveAt: effectiveAt,
                eventsById: eventsById,
                rootEventIds: rootEventsId,
                offset: offset,
                transactionId: transactionId
            }
            if (commandId) {
                transactionTree.commandId = commandId;
            }
            if (workflowId) {
                transactionTree.workflowId = workflowId;
            }
            return transactionTree;
        },
        (transactionTree) =>
            [
                transactionTree.commandId,
                transactionTree.effectiveAt,
                transactionTree.eventsById,
                transactionTree.rootEventIds,
                transactionTree.offset,
                transactionTree.transactionId,
                transactionTree.workflowId
            ]
    );