// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { maybe } from './maybe';
import { Status } from './status';

export const Completion: jsc.Arbitrary<ledger.Completion> =
    jsc.pair(jsc.string, maybe(Status)).smap<ledger.Completion>(
        ([commandId, status]) => ({
            commandId: commandId,
            status: status
        })
        ,
        (completion) =>
            [completion.commandId, completion.status]
    );