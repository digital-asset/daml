// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Filters } from './filters';

export const TransactionFilter: jsc.Arbitrary<ledger.TransactionFilter> =
    jsc.dict(Filters).smap(
        (filtersByParty) => {
            return {
                filtersByParty: filtersByParty
            }
        },
        (transactionFilter) =>
            transactionFilter.filtersByParty
    );