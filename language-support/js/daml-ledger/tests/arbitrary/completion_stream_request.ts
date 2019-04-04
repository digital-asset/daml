// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { LedgerOffset } from './ledger_offset';

export const CompletionStreamRequest: jsc.Arbitrary<ledger.CompletionStreamRequest> =
    jsc.tuple([jsc.string, LedgerOffset, jsc.array(jsc.string)]).smap<ledger.CompletionStreamRequest>(
        ([applicationId, offset, parties]) => ({
            applicationId: applicationId,
            offset: offset,
            parties: parties
        }),
        (request) =>
            [request.applicationId, request.offset, request.parties]
    );