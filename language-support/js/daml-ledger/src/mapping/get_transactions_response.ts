// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTransactionsResponse: mapping.Mapping<grpc.GetTransactionsResponse, ledger.GetTransactionsResponse> = {
    toObject(message: grpc.GetTransactionsResponse): ledger.GetTransactionsResponse {
        return {
            transactions: message.getTransactionsList().map((t: grpc.Transaction) => mapping.Transaction.toObject(t))
        };
    },
    toMessage(object: ledger.GetTransactionsResponse): grpc.GetTransactionsResponse {
        const message = new grpc.GetTransactionsResponse();
        message.setTransactionsList(object.transactions.map((t: ledger.Transaction) => mapping.Transaction.toMessage(t)))
        return message;
    }
}