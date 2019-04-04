// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { TransactionTree } from './transaction_tree';

export const GetTransactionResponse: jsc.Arbitrary<ledger.GetTransactionResponse> =
    TransactionTree.smap<ledger.GetTransactionResponse>(
        transaction => ({
            transaction: transaction,
        }),
        request =>
            request.transaction
    );