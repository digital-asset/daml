// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { LedgerOffset } from './ledger_offset';
import { maybe } from './maybe';
import { TransactionFilter } from './transaction_filter';

export const GetTransactionsRequest: jsc.Arbitrary<ledger.GetTransactionsRequest> =
    jsc.tuple([LedgerOffset, maybe(LedgerOffset), TransactionFilter, maybe(jsc.bool)]).smap(
        ([begin, end, filter, verbose]) => {
            const request: ledger.GetTransactionsRequest = {
                begin: begin,
                filter: filter
            }
            if (end) {
                request.end = end;
            }
            if (verbose !== undefined) {
                request.verbose = verbose;
            }
            return request;
        },
        (request) => {
            return [request.begin, request.end, request.filter, request.verbose];
        }
    );