// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetActiveContractsRequest: mapping.Mapping<grpc.GetActiveContractsRequest, ledger.GetActiveContractsRequest> = {
    toObject(message: grpc.GetActiveContractsRequest): ledger.GetActiveContractsRequest {
        const object: ledger.GetActiveContractsRequest = {
            filter: mapping.TransactionFilter.toObject(message.getFilter()!)
        }
        const verbose = message.getVerbose();
        if (verbose !== undefined) {
            object.verbose = verbose;
        }
        return object;
    },
    toMessage(object: ledger.GetActiveContractsRequest): grpc.GetActiveContractsRequest {
        const result = new grpc.GetActiveContractsRequest();
        if (object.verbose) {
            result.setVerbose(object.verbose);
        }
        result.setFilter(mapping.TransactionFilter.toMessage(object.filter));
        return result;
    }
}