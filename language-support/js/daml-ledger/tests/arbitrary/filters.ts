// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from '../../src';

import * as jsc from 'jsverify';
import { InclusiveFilters } from './inclusive_filters';
import { maybe } from './maybe';

export const Filters: jsc.Arbitrary<ledger.Filters> =
    maybe(InclusiveFilters).smap<ledger.Filters>(
        (inclusive) => {
            return inclusive ? { inclusive: inclusive } : {};
        },
        (filters) => {
            return filters.inclusive
        }
    );