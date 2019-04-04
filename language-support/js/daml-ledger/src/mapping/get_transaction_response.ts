// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTransactionResponse: mapping.Mapping<grpc.GetTransactionResponse, ledger.GetTransactionResponse> = {
    toObject(message: grpc.GetTransactionResponse): ledger.GetTransactionResponse {
        return {
            transaction: mapping.TransactionTree.toObject(message.getTransaction()!)
        };
    },
    toMessage(object: ledger.GetTransactionResponse): grpc.GetTransactionResponse {
        const message = new grpc.GetTransactionResponse();
        message.setTransaction(mapping.TransactionTree.toMessage(object.transaction));
        return message;
    }
}