// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Value } from './record_value_variant';
import { maybe } from './maybe'

export const Optional: jsc.Arbitrary<ledger.Optional> =
    maybe(Value).smap<ledger.Optional>(
        value => ({
            value: value,
        }),
        request =>
            request.value
    );