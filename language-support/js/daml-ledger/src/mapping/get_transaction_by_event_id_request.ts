// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTransactionByEventIdRequest: mapping.Mapping<grpc.GetTransactionByEventIdRequest, ledger.GetTransactionByEventIdRequest> = {
    toObject(message: grpc.GetTransactionByEventIdRequest): ledger.GetTransactionByEventIdRequest {
        return {
            eventId: message.getEventId(),
            requestingParties: message.getRequestingPartiesList()
        };
    },
    toMessage(object: ledger.GetTransactionByEventIdRequest): grpc.GetTransactionByEventIdRequest {
        const message = new grpc.GetTransactionByEventIdRequest();
        message.setEventId(object.eventId);
        message.setRequestingPartiesList(object.requestingParties);
        return message;
    }
}