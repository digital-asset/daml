// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetLedgerConfigurationResponse: mapping.Mapping<grpc.GetLedgerConfigurationResponse, ledger.GetLedgerConfigurationResponse> = {
    toObject(message: grpc.GetLedgerConfigurationResponse): ledger.GetLedgerConfigurationResponse {
        return {
            config: mapping.LedgerConfiguration.toObject(message.getLedgerConfiguration()!)
        };
    },
    toMessage(object: ledger.GetLedgerConfigurationResponse): grpc.GetLedgerConfigurationResponse {
        const message = new grpc.GetLedgerConfigurationResponse();
        message.setLedgerConfiguration(mapping.LedgerConfiguration.toMessage(object.config));
        return message;
    }
}