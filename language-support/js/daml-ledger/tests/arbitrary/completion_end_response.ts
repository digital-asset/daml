// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { LedgerOffset } from './ledger_offset';

export const CompletionEndResponse: jsc.Arbitrary<ledger.CompletionEndResponse> =
    LedgerOffset.smap<ledger.CompletionEndResponse>(
        offset => ({
            offset: offset,
        }),
        request =>
            request.offset
    );