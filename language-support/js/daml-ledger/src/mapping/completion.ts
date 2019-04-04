// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Completion: mapping.Mapping<grpc.Completion, ledger.Completion> = {
    toObject(message: grpc.Completion): ledger.Completion {
        const completion: ledger.Completion = {
            commandId: message.getCommandId()
        }
        const status = message.getStatus();
        if (status !== undefined) {
            completion.status = mapping.Status.toObject(status);
        }
        return completion;
    },
    toMessage(object: ledger.Completion): grpc.Completion {
        const message = new grpc.Completion();
        message.setCommandId(object.commandId);
        if (object.status) {
            message.setStatus(mapping.Status.toMessage(object.status));
        }
        return message;
    }
}