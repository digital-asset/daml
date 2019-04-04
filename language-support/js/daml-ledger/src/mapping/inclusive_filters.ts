// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const InclusiveFilters: mapping.Mapping<grpc.InclusiveFilters, ledger.InclusiveFilters> = {
    toObject(inclusiveFilters: grpc.InclusiveFilters): ledger.InclusiveFilters {
        return {
            templateIds: inclusiveFilters.getTemplateIdsList().map((id) => mapping.Identifier.toObject(id))
        }
    },
    toMessage(inclusiveFilters: ledger.InclusiveFilters): grpc.InclusiveFilters {
        const result = new grpc.InclusiveFilters();
        result.setTemplateIdsList(inclusiveFilters.templateIds.map((id) => mapping.Identifier.toMessage(id)));
        return result;
    }
};