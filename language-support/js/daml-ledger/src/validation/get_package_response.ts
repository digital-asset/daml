// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { native, object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { HashFunction } from "./hash_function";

function required(): RequiredValidation<ledger.GetPackageResponse> {
    return {
        archivePayload: native('string'),
        hash: native('string'),
        hashFunction: HashFunction
    };
}

export const GetPackageResponse: Validation = object<ledger.GetPackageResponse>('GetPackageResponse', required, () => ({}));