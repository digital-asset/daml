// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const SetTimeRequest: mapping.Mapping<grpc.testing.SetTimeRequest, ledger.SetTimeRequest> = {
    toObject(message: grpc.testing.SetTimeRequest): ledger.SetTimeRequest {
        return {
            currentTime: mapping.Timestamp.toObject(message.getCurrentTime()!),
            newTime: mapping.Timestamp.toObject(message.getNewTime()!)
        };
    },
    toMessage(object: ledger.SetTimeRequest): grpc.testing.SetTimeRequest {
        const message = new grpc.testing.SetTimeRequest();
        message.setCurrentTime(mapping.Timestamp.toMessage(object.currentTime));
        message.setNewTime(mapping.Timestamp.toMessage(object.newTime));
        return message;
    }
}