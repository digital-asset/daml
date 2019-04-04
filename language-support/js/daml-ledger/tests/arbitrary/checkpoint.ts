// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { LedgerOffset } from './ledger_offset';
import { Timestamp } from './timestamp';

export const Checkpoint: jsc.Arbitrary<ledger.Checkpoint> =
    jsc.pair(LedgerOffset, Timestamp).smap<ledger.Checkpoint>(
        ([offset, recordTime]) => {
            return {
                offset: offset,
                recordTime: recordTime
            }
        },
        (checkpoint) => {
            return [checkpoint.offset, checkpoint.recordTime]
        }
    );