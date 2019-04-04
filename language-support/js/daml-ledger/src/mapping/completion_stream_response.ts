// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const CompletionStreamResponse: mapping.Mapping<grpc.CompletionStreamResponse, ledger.CompletionStreamResponse> = {
    toObject(message: grpc.CompletionStreamResponse): ledger.CompletionStreamResponse {
        const response: ledger.CompletionStreamResponse = {};
        const completions = message.getCompletionsList();
        if (completions) {
            response.completions = completions.map(c => mapping.Completion.toObject(c));
        }
        if (message.hasCheckpoint()) {
            response.checkpoint = mapping.Checkpoint.toObject(message.getCheckpoint()!)
        }
        return response;
    },
    toMessage(object: ledger.CompletionStreamResponse): grpc.CompletionStreamResponse {
        const result = new grpc.CompletionStreamResponse();
        if (object.checkpoint) {
            result.setCheckpoint(mapping.Checkpoint.toMessage(object.checkpoint));
        }
        if (object.completions) {
            result.setCompletionsList(object.completions.map((c: ledger.Completion) => mapping.Completion.toMessage(c)));
        }
        return result;
    }
}