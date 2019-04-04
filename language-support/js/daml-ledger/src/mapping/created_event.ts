// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const CreatedEvent: mapping.Mapping<grpc.CreatedEvent, ledger.CreatedEvent> = {
    toObject(event: grpc.CreatedEvent): ledger.CreatedEvent {
        return {
            eventId: event.getEventId(),
            contractId: event.getContractId(),
            templateId: mapping.Identifier.toObject(event.getTemplateId()!),
            arguments: mapping.Record.toObject(event.getCreateArguments()!),
            witnessParties: event.getWitnessPartiesList()
        };
    },
    toMessage(event: ledger.CreatedEvent): grpc.CreatedEvent {
        const result = new grpc.CreatedEvent();
        result.setEventId(event.eventId);
        result.setContractId(event.contractId);
        result.setTemplateId(mapping.Identifier.toMessage(event.templateId));
        result.setCreateArguments(mapping.Record.toMessage(event.arguments));
        result.setWitnessPartiesList(event.witnessParties);
        return result;
    }
}