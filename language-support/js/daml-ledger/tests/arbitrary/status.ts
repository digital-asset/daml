// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Any } from './any';

export const Status: jsc.Arbitrary<ledger.Status> =
    jsc.tuple([jsc.number, jsc.array(Any), jsc.string]).smap<ledger.Status>(
        ([code, details, message]) => ({
            code: code,
            details: details,
            message: message
        }),
        status =>
            [status.code, status.details, status.message]
    );