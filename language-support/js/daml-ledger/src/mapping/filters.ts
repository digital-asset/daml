// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Filters: mapping.Mapping<grpc.Filters, ledger.Filters> = {
    toObject(filters: grpc.Filters): ledger.Filters {
        let result: ledger.Filters = {};
        if (filters.hasInclusive()) {
            result.inclusive = mapping.InclusiveFilters.toObject(filters.getInclusive()!);
        }
        return result;
    },
    toMessage(filter: ledger.Filters): grpc.Filters {
        const result = new grpc.Filters();
        if (filter.inclusive) {
            result.setInclusive(mapping.InclusiveFilters.toMessage(filter.inclusive));
        }
        return result;
    }
}