// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const Any: jsc.Arbitrary<ledger.Any> =
    jsc.pair(jsc.string, jsc.string).smap<ledger.Any>(
        ([typeUrl, value]) => {
            return {
                typeUrl: typeUrl,
                value: value
            }
        },
        (any) => {
            return [any.typeUrl, any.typeUrl]
        }
    );