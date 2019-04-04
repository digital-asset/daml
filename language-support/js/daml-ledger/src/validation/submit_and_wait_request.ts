// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { object, Validation } from "./base";
import { RequiredValidation } from "./base/typelevel";
import { Commands } from "./commands";

function required(): RequiredValidation<ledger.SubmitAndWaitRequest> {
    return {
        commands: Commands
    };
}

export const SubmitAndWaitRequest: Validation = object<ledger.SubmitAndWaitRequest>('SubmitAndWaitRequest', required, () => ({}));