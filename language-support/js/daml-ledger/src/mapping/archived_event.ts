// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const ArchivedEvent: mapping.Mapping<grpc.ArchivedEvent, ledger.ArchivedEvent> = {
    toObject(message: grpc.ArchivedEvent): ledger.ArchivedEvent {
        return {
            contractId: message.getContractId(),
            eventId: message.getEventId(),
            templateId: mapping.Identifier.toObject(message.getTemplateId()!),
            witnessParties: message.getWitnessPartiesList()
        };
    },
    toMessage(object: ledger.ArchivedEvent): grpc.ArchivedEvent {
        const message = new grpc.ArchivedEvent();
        message.setContractId(object.contractId);
        message.setEventId(object.eventId);
        message.setTemplateId(mapping.Identifier.toMessage(object.templateId));
        message.setWitnessPartiesList(object.witnessParties);
        return message;
    }
}