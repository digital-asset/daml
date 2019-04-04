// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const ListPackagesResponse: jsc.Arbitrary<ledger.ListPackagesResponse> =
    jsc.array(jsc.string).smap<ledger.ListPackagesResponse>(
        packageIds => ({
            packageIds: packageIds,
        }),
        request =>
            request.packageIds
    );