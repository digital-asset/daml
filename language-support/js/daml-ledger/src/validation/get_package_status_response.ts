// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { PackageStatus } from "./package_status";

function required(): RequiredValidation<ledger.GetPackageStatusResponse> {
    return {
        status: PackageStatus
    };
}

export const GetPackageStatusResponse: Validation = object<ledger.GetPackageStatusResponse>('GetPackageStatusResponse', required, () => ({}));