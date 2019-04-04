// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Duration } from './duration';

export const LedgerConfiguration: jsc.Arbitrary<ledger.LedgerConfiguration> =
    jsc.pair(Duration, Duration).smap<ledger.LedgerConfiguration>(
        ([maxTtl, minTtl]) => {
            return {
                maxTtl: maxTtl,
                minTtl: minTtl
            }
        },
        (ledgerConfiguration) => {
            return [ledgerConfiguration.maxTtl, ledgerConfiguration.minTtl]
        }
    );