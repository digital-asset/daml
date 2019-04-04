// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Command } from './command';
import { maybe } from './maybe';
import { Timestamp } from './timestamp';

export const Commands: jsc.Arbitrary<ledger.Commands> =
    jsc.tuple([jsc.string, jsc.string, Timestamp, jsc.array(Command), Timestamp, jsc.string, maybe(jsc.string)]).smap<ledger.Commands>(
        ([applicationId, commandId, ledgerEffectiveTime, list, maximumRecordTime, party, workflowId]) => ({
            applicationId: applicationId,
            commandId: commandId,
            ledgerEffectiveTime: ledgerEffectiveTime,
            list: list,
            maximumRecordTime: maximumRecordTime,
            party: party,
            workflowId: workflowId
        }),
        (commands) =>
            [commands.applicationId, commands.commandId, commands.ledgerEffectiveTime, commands.list, commands.maximumRecordTime, commands.party, commands.workflowId]
    );