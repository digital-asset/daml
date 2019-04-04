// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const SubmitRequest: mapping.Mapping<grpc.SubmitRequest, ledger.SubmitRequest> = {
    toObject(request: grpc.SubmitRequest): ledger.SubmitRequest {
        return {
            commands: mapping.Commands.toObject(request.getCommands()!)
        };
    },
    toMessage(request: ledger.SubmitRequest): grpc.SubmitRequest {
        const result = new grpc.SubmitRequest();
        result.setCommands(mapping.Commands.toMessage(request.commands));
        return result;
    }
}