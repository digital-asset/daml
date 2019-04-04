// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const CompletionEndResponse: mapping.Mapping<grpc.CompletionEndResponse, ledger.CompletionEndResponse> = {
    toObject(message: grpc.CompletionEndResponse): ledger.CompletionEndResponse {
        return {
            offset: mapping.LedgerOffset.toObject(message.getOffset()!)
        };
    },
    toMessage(response: ledger.CompletionEndResponse): grpc.CompletionEndResponse {
        const result = new grpc.CompletionEndResponse();
        result.setOffset(mapping.LedgerOffset.toMessage(response.offset));
        return result;
    }
}