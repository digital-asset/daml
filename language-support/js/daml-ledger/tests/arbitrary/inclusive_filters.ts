// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

import { Identifier } from './identifier';

export const InclusiveFilters: jsc.Arbitrary<ledger.InclusiveFilters> =
    jsc.array(Identifier).smap<ledger.InclusiveFilters>(
        (templateIds) => {
            return {
                templateIds: templateIds
            }
        },
        (inclusiveFilters) => {
            return inclusiveFilters.templateIds
        }
    );