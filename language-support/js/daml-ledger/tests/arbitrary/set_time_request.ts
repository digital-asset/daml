// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Timestamp } from './timestamp';

export const SetTimeRequest: jsc.Arbitrary<ledger.SetTimeRequest> =
    jsc.tuple([Timestamp, Timestamp]).smap<ledger.SetTimeRequest>(
        ([currentTime, newTime]) => ({
            currentTime: currentTime,
            newTime: newTime,
        }),
        (request) =>
            [request.currentTime, request.newTime]
    );