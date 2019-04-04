// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const TreeEvent: mapping.Mapping<grpc.TreeEvent, ledger.TreeEvent> = {
    toObject(message: grpc.TreeEvent): ledger.TreeEvent {
        const object: ledger.TreeEvent = {};
        if (message.hasCreated()) {
            object.created = mapping.CreatedEvent.toObject(message.getCreated()!);
        }
        if (message.hasExercised()) {
            object.exercised = mapping.ExercisedEvent.toObject(message.getExercised()!);
        }
        return object;
    },
    toMessage(object: ledger.TreeEvent): grpc.TreeEvent {
        const message = new grpc.TreeEvent();
        if (object.created) {
            message.setCreated(mapping.CreatedEvent.toMessage(object.created));
        }
        if (object.exercised) {
            message.setExercised(mapping.ExercisedEvent.toMessage(object.exercised));
        }
        return message;
    }
}