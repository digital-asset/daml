// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Timestamp } from './timestamp';
import { Event } from './event';
import { maybe } from './maybe';

export const Transaction: jsc.Arbitrary<ledger.Transaction> =
    jsc.tuple([maybe(jsc.string), Timestamp, jsc.array(Event), jsc.string, jsc.string, maybe(jsc.string)]).smap<ledger.Transaction>(
        ([commandId, effectiveAt, events, offset, transactionId, workflowId]) => {
            const transaction: ledger.Transaction = {
                effectiveAt: effectiveAt,
                events: events,
                offset: offset,
                transactionId: transactionId
            }
            if (commandId) {
                transaction.commandId = commandId;
            }
            if (workflowId) {
                transaction.workflowId = workflowId;
            }
            return transaction;
        },
        (transaction) =>
            [transaction.commandId, transaction.effectiveAt, transaction.events, transaction.offset, transaction.transactionId, transaction.workflowId]
    );