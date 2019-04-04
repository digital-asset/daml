// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTransactionsRequest: mapping.Mapping<grpc.GetTransactionsRequest, ledger.GetTransactionsRequest> = {
    toObject(request: grpc.GetTransactionsRequest): ledger.GetTransactionsRequest {
        const result: ledger.GetTransactionsRequest = {
            begin: mapping.LedgerOffset.toObject(request.getBegin()!),
            filter: mapping.TransactionFilter.toObject(request.getFilter()!)
        };
        if (request.hasEnd()) {
            result.end = mapping.LedgerOffset.toObject(request.getEnd()!);
        }
        const verbose = request.getVerbose();
        if (verbose !== undefined) {
            result.verbose = verbose;
        }
        return result;
    },
    toMessage(request: ledger.GetTransactionsRequest): grpc.GetTransactionsRequest {
        const result = new grpc.GetTransactionsRequest();
        result.setBegin(mapping.LedgerOffset.toMessage(request.begin));
        if (request.end) {
            result.setEnd(mapping.LedgerOffset.toMessage(request.end));
        }
        result.setFilter(mapping.TransactionFilter.toMessage(request.filter));
        if (request.verbose !== undefined) {
            result.setVerbose(request.verbose);
        }
        return result;
    }
}