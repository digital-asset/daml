// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const Identifier: jsc.Arbitrary<ledger.Identifier> =
    jsc.pair(jsc.string, jsc.string).smap<ledger.Identifier>(
        ([name, packageId]) => {
            return {
                name: name,
                packageId: packageId
            }
        },
        (identifier) => {
            return [identifier.name, identifier.packageId]
        }
    );