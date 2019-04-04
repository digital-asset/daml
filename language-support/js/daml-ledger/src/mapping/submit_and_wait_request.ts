// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const SubmitAndWaitRequest: mapping.Mapping<grpc.SubmitAndWaitRequest, ledger.SubmitAndWaitRequest> = {
    toObject(request: grpc.SubmitAndWaitRequest): ledger.SubmitAndWaitRequest {
        return {
            commands: mapping.Commands.toObject(request.getCommands()!)
        };
    },
    toMessage(request: ledger.SubmitAndWaitRequest): grpc.SubmitAndWaitRequest {
        const result = new grpc.SubmitAndWaitRequest();
        result.setCommands(mapping.Commands.toMessage(request.commands));
        return result;
    }
}