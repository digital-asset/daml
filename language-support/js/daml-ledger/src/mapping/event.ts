// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

import { inspect } from 'util';

export const Event: mapping.Mapping<grpc.Event, ledger.Event> = {
    toObject(message: grpc.Event): ledger.Event {
        if (message.hasArchived()) {
            return {
                archived: mapping.ArchivedEvent.toObject(message.getArchived()!)
            }
        } else if (message.hasCreated()) {
            return {
                created: mapping.CreatedEvent.toObject(message.getCreated()!)
            }
        } else if (message.hasExercised()) {
            return {
                exercised: mapping.ExercisedEvent.toObject(message.getExercised()!)
            }
        } else {
            throw new Error(`Expected one of ArchivedEvent, CreatedEvent or ExercisedEvent, found ${inspect(message)}`);
        }
    },
    toMessage(object: ledger.Event): grpc.Event {
        const message = new grpc.Event();
        if (object.archived !== undefined) {
            message.setArchived(mapping.ArchivedEvent.toMessage(object.archived));
        } else if (object.created !== undefined) {
            message.setCreated(mapping.CreatedEvent.toMessage(object.created));
        } else if (object.exercised !== undefined) {
            message.setExercised(mapping.ExercisedEvent.toMessage(object.exercised));
        } else {
            throw new Error(`Expected one of ArchivedEvent, CreatedEvent or ExercisedEvent, found ${inspect(object)}`);
        }
        return message;
    }
}