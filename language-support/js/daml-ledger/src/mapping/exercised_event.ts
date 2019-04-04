// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const ExercisedEvent: mapping.Mapping<grpc.ExercisedEvent, ledger.ExercisedEvent> = {
    toObject(message: grpc.ExercisedEvent): ledger.ExercisedEvent {
        const event: ledger.ExercisedEvent = {
            actingParties: message.getActingPartiesList(),
            choice: message.getChoice(),
            argument: mapping.Value.toObject(message.getChoiceArgument()!),
            consuming: message.getConsuming(),
            contractCreatingEventId: message.getContractCreatingEventId(),
            contractId: message.getContractId(),
            eventId: message.getEventId(),
            templateId: mapping.Identifier.toObject(message.getTemplateId()!),
            witnessParties: message.getWitnessPartiesList()
        };
        const childEventIds = message.getChildEventIdsList();
        if (childEventIds) {
            event.childEventIds = [...childEventIds];
        }
        return event;
    },
    toMessage(object: ledger.ExercisedEvent): grpc.ExercisedEvent {
        const message = new grpc.ExercisedEvent();
        message.setActingPartiesList(object.actingParties);
        message.setChoice(object.choice);
        message.setChoiceArgument(mapping.Value.toMessage(object.argument));
        message.setConsuming(object.consuming);
        message.setContractCreatingEventId(object.contractCreatingEventId);
        message.setContractId(object.contractId);
        message.setEventId(object.eventId);
        message.setTemplateId(mapping.Identifier.toMessage(object.templateId));
        message.setWitnessPartiesList(object.witnessParties)
        if (object.childEventIds) {
            message.setChildEventIdsList([...object.childEventIds]);
        }
        return message;
    }
}