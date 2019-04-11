// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Identifier: mapping.Mapping<grpc.Identifier, ledger.Identifier> = {
    toObject(identifier: grpc.Identifier): ledger.Identifier {
        return {
            packageId: identifier.getPackageId(),
            moduleName: identifier.getModuleName(),
            entityName: identifier.getEntityName()
        };
    },
    toMessage(identifier: ledger.Identifier): grpc.Identifier {
        const value = new grpc.Identifier();
        value.setPackageId(identifier.packageId);
        value.setModuleName(identifier.moduleName);
        value.setEntityName(identifier.entityName);
        return value;
    }
};