// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTransactionByIdRequest: mapping.Mapping<grpc.GetTransactionByIdRequest, ledger.GetTransactionByIdRequest> = {
    toObject(message: grpc.GetTransactionByIdRequest): ledger.GetTransactionByIdRequest {
        return {
            transactionId: message.getTransactionId(),
            requestingParties: message.getRequestingPartiesList()
        };
    },
    toMessage(object: ledger.GetTransactionByIdRequest): grpc.GetTransactionByIdRequest {
        const message = new grpc.GetTransactionByIdRequest();
        message.setTransactionId(object.transactionId);
        message.setRequestingPartiesList(object.requestingParties);
        return message;
    }
}