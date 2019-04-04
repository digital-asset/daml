// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Transaction } from './transaction';

export const GetTransactionsResponse: jsc.Arbitrary<ledger.GetTransactionsResponse> =
    jsc.array(Transaction).smap<ledger.GetTransactionsResponse>(
        transactions => ({
            transactions: transactions,
        }),
        request =>
            request.transactions
    );