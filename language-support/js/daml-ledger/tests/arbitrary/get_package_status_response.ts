// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';

export const GetPackageStatusResponse: jsc.Arbitrary<ledger.GetPackageStatusResponse> =
    jsc.elements([ledger.PackageStatus.REGISTERED, ledger.PackageStatus.UNKNOWN]).smap<ledger.GetPackageStatusResponse>(
        status => ({
            status: status,
        }),
        request =>
            request.status
    );