// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Timestamp } from './timestamp';

export const GetTimeResponse: jsc.Arbitrary<ledger.GetTimeResponse> =
    Timestamp.smap<ledger.GetTimeResponse>(
        currentTime => ({
            currentTime: currentTime,
        }),
        request =>
            request.currentTime
    );