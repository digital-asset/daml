// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTransactionTreesResponse: mapping.Mapping<grpc.GetTransactionTreesResponse, ledger.GetTransactionTreesResponse> = {
    toObject(message: grpc.GetTransactionTreesResponse): ledger.GetTransactionTreesResponse {
        return {
            transactions: message.getTransactionsList().map(t => mapping.TransactionTree.toObject(t))
        };
    },
    toMessage(object: ledger.GetTransactionTreesResponse): grpc.GetTransactionTreesResponse {
        const message = new grpc.GetTransactionTreesResponse();
        message.setTransactionsList(object.transactions.map(t => mapping.TransactionTree.toMessage(t)))
        return message;
    }
}