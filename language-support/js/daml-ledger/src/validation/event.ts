// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from "..";
import { ArchivedEvent } from "./archived_event";
import { union, Validation } from "./base";
import { CreatedEvent } from "./created_event";
import { ExercisedEvent } from "./exercised_event";

function values(): Record<keyof ledger.Event, Validation> {
    return {
        archived: ArchivedEvent,
        created: CreatedEvent,
        exercised: ExercisedEvent
    };
}

export const Event: Validation = union('Event', values);