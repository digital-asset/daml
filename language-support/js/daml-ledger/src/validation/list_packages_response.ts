// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation, native, array } from "./base";
import { RequiredValidation } from "./base/typelevel";

function required(): RequiredValidation<ledger.ListPackagesResponse> {
    return {
        packageIds: array(native('string'))
    };
}

export const ListPackagesResponse: Validation = object<ledger.ListPackagesResponse>('ListPackagesResponse', required, () => ({}));