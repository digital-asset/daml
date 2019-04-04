// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const GetLedgerIdentityResponse: jsc.Arbitrary<ledger.GetLedgerIdentityResponse> =
    jsc.string.smap<ledger.GetLedgerIdentityResponse>(
        ledgerId => ({
            ledgerId: ledgerId,
        }),
        request =>
            request.ledgerId
    );