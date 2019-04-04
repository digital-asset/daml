// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { LedgerOffset } from './ledger_offset';

export const GetLedgerEndResponse: jsc.Arbitrary<ledger.GetLedgerEndResponse> =
    LedgerOffset.smap<ledger.GetLedgerEndResponse>(
        offset => ({
            offset: offset,
        }),
        request =>
            request.offset
    );