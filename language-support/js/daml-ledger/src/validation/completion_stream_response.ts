// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { array, object, Validation } from "./base";
import { OptionalValidation } from "./base/typelevel";
import { Checkpoint } from "./checkpoint";
import { Completion } from "./completion";

function optional(): OptionalValidation<ledger.CompletionStreamResponse> {
    return {
        checkpoint: Checkpoint,
        completions: array(Completion),
    };
}

export const CompletionStreamResponse: Validation = object<ledger.CompletionStreamResponse>('CompletionStreamResponse', () => ({}), optional);