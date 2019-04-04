// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { maybe } from './maybe';
import { TransactionFilter } from './transaction_filter';

export const GetActiveContractsRequest: jsc.Arbitrary<ledger.GetActiveContractsRequest> =
    jsc.tuple([TransactionFilter, maybe(jsc.bool)]).smap(
        ([filter, verbose]) => {
            const request: ledger.GetActiveContractsRequest = {
                filter: filter
            }
            if (verbose) {
                request.verbose = verbose;
            }
            return request;
        },
        (request) => {
            return [request.filter, request.verbose];
        }
    );