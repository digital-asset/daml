// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { union, Validation } from "./base";
import * as ledger from "..";
import { CreatedEvent } from "./created_event";
import { ExercisedEvent } from "./exercised_event";

function values(): Record<keyof ledger.TreeEvent, Validation> {
    return {
        created: CreatedEvent,
        exercised: ExercisedEvent
    };
}

export const TreeEvent: Validation = union<ledger.TreeEvent>("TreeEvent", values)