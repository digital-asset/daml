// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const CompletionStreamRequest: mapping.Mapping<grpc.CompletionStreamRequest, ledger.CompletionStreamRequest> = {
    toObject(message: grpc.CompletionStreamRequest): ledger.CompletionStreamRequest {
        return {
            applicationId: message.getApplicationId(),
            offset: mapping.LedgerOffset.toObject(message.getOffset()!),
            parties: message.getPartiesList()
        };
    },
    toMessage(object: ledger.CompletionStreamRequest): grpc.CompletionStreamRequest {
        const result = new grpc.CompletionStreamRequest();
        result.setApplicationId(object.applicationId);
        result.setOffset(mapping.LedgerOffset.toMessage(object.offset));
        result.setPartiesList(object.parties);
        return result;
    }
}