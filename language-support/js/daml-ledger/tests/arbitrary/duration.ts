// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const Duration: jsc.Arbitrary<ledger.Duration> =
    jsc.pair(jsc.number, jsc.number).smap<ledger.Duration>(
        ([seconds, nanoseconds]) => {
            return {
                seconds: seconds,
                nanoseconds: nanoseconds
            }
        },
        (timestamp) => {
            return [timestamp.seconds, timestamp.nanoseconds]
        }
    );