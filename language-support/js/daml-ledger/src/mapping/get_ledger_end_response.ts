// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetLedgerEndResponse: mapping.Mapping<grpc.GetLedgerEndResponse, ledger.GetLedgerEndResponse> = {
    toObject(message: grpc.GetLedgerEndResponse): ledger.GetLedgerEndResponse {
        return {
            offset: mapping.LedgerOffset.toObject(message.getOffset()!)
        };
    },
    toMessage(object: ledger.GetLedgerEndResponse): grpc.GetLedgerEndResponse {
        const message = new grpc.GetLedgerEndResponse();
        message.setOffset(mapping.LedgerOffset.toMessage(object.offset));
        return message;
    }
};