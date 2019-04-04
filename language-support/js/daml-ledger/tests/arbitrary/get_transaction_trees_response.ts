// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { TransactionTree } from './transaction_tree';

export const GetTransactionTreesResponse: jsc.Arbitrary<ledger.GetTransactionTreesResponse> =
    jsc.array(TransactionTree).smap<ledger.GetTransactionTreesResponse>(
        transactions => ({
            transactions: transactions,
        }),
        request =>
            request.transactions
    );