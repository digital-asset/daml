// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const GetTransactionByEventIdRequest: jsc.Arbitrary<ledger.GetTransactionByEventIdRequest> =
    jsc.tuple([jsc.string, jsc.array(jsc.string)]).smap(
        ([eventId, requestingParties]) => ({
            eventId: eventId,
            requestingParties: requestingParties
        }),
        (request) =>
            [request.eventId, request.requestingParties]
    );