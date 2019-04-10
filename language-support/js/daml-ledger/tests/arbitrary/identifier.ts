// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const Identifier: jsc.Arbitrary<ledger.Identifier> =
    jsc.tuple([jsc.string, jsc.string, jsc.string]).smap<ledger.Identifier>(
        ([packageId, moduleName, entityName]) => {
            return {
                packageId: packageId,
                moduleName: moduleName,
                entityName: entityName
            }
        },
        (identifier) => {
            return [identifier.packageId, identifier.moduleName, identifier.entityName]
        }
    );