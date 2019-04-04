// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as jsc from 'jsverify';
import * as ledger from '../../src';
import { Identifier } from './identifier';
import { Record, Value } from './record_value_variant';
import { maybe } from './maybe';

export const ArchivedEvent: jsc.Arbitrary<ledger.ArchivedEvent> =
    jsc.tuple([jsc.string, jsc.string, Identifier, jsc.array(jsc.string)]).smap<ledger.ArchivedEvent>(
        ([contractId, eventId, templateId, witnessParties]) => {
            return {
                contractId: contractId,
                eventId: eventId,
                templateId: templateId,
                witnessParties: witnessParties
            }
        },
        (archivedEvent) => {
            return [archivedEvent.contractId, archivedEvent.eventId, archivedEvent.templateId, archivedEvent.witnessParties]
        }
    );

export const CreatedEvent: jsc.Arbitrary<ledger.CreatedEvent> =
    jsc.tuple([jsc.string, jsc.string, Identifier, Record, jsc.array(jsc.string)]).smap<ledger.CreatedEvent>(
        ([contractId, eventId, templateId, args, witnessParties]) => {
            return {
                contractId: contractId,
                eventId: eventId,
                templateId: templateId,
                arguments: args,
                witnessParties: witnessParties
            }
        },
        (createdEvent) => {
            return [createdEvent.contractId, createdEvent.eventId, createdEvent.templateId, createdEvent.arguments, createdEvent.witnessParties]
        }
    );

export const ExercisedEvent: jsc.Arbitrary<ledger.ExercisedEvent> =
    jsc.tuple([jsc.array(jsc.string), maybe(jsc.array(jsc.string)), jsc.string, Value, jsc.bool, jsc.string, jsc.string, jsc.string, Identifier, jsc.array(jsc.string)]).smap<ledger.ExercisedEvent>(
        ([actingParties, childEventIds, choice, argument, consuming, contractCreatingEventId, contractId, eventId, templateId, witnessParties]) => ({
            actingParties: actingParties,
            childEventIds: childEventIds,
            choice: choice,
            argument: argument,
            consuming: consuming,
            contractCreatingEventId: contractCreatingEventId,
            contractId: contractId,
            eventId: eventId,
            templateId: templateId,
            witnessParties: witnessParties
        }), (exercisedEvent) =>
            [
                exercisedEvent.actingParties,
                exercisedEvent.childEventIds,
                exercisedEvent.choice,
                exercisedEvent.argument,
                exercisedEvent.consuming,
                exercisedEvent.contractCreatingEventId,
                exercisedEvent.contractId,
                exercisedEvent.eventId,
                exercisedEvent.templateId,
                exercisedEvent.witnessParties
            ]
        );

const Archived: jsc.Arbitrary<ledger.Event> =
    ArchivedEvent.smap<{ archived: ledger.ArchivedEvent }>(
        archived => ({ archived: archived }),
        event => event.archived
    );
const Created: jsc.Arbitrary<ledger.Event> =
    CreatedEvent.smap<{ created: ledger.CreatedEvent }>(
        created => ({ created: created }),
        event => event.created
    );
const Exercised: jsc.Arbitrary<ledger.Event> =
    ExercisedEvent.smap<{ exercised: ledger.ExercisedEvent }>(
        exercised => ({ exercised: exercised }),
        event => event.exercised
    );

export const Event = jsc.oneof([Archived, Created, Exercised]);
export const TreeEvent = jsc.oneof([Created, Exercised]);