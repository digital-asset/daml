// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const LedgerConfiguration: mapping.Mapping<grpc.LedgerConfiguration, ledger.LedgerConfiguration> = {
    toObject(message: grpc.LedgerConfiguration): ledger.LedgerConfiguration {
        return {
            minTtl: mapping.Duration.toObject(message.getMinTtl()!),
            maxTtl: mapping.Duration.toObject(message.getMaxTtl()!)
        };
    },
    toMessage(object: ledger.LedgerConfiguration): grpc.LedgerConfiguration {
        const message = new grpc.LedgerConfiguration();
        message.setMinTtl(mapping.Duration.toMessage(object.minTtl));
        message.setMaxTtl(mapping.Duration.toMessage(object.maxTtl));
        return message;
    }
}