// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const TransactionFilter: mapping.Mapping<grpc.TransactionFilter, ledger.TransactionFilter> = {
    toObject(transactionFilter: grpc.TransactionFilter): ledger.TransactionFilter {
        let filtersByParty: {[k: string]: ledger.Filters} = {};
        transactionFilter.getFiltersByPartyMap().forEach((filters: grpc.Filters, party: string) => {
            filtersByParty[party] = mapping.Filters.toObject(filters);
        });
        return { filtersByParty: filtersByParty };
    },
    toMessage(transactionFilter: ledger.TransactionFilter): grpc.TransactionFilter {
        const result = new grpc.TransactionFilter();
        const map = result.getFiltersByPartyMap();
        Object.keys(transactionFilter.filtersByParty).forEach((party: string) => {
            map.set(party, mapping.Filters.toMessage(transactionFilter.filtersByParty[party]));
        });
        return result;
    }
}