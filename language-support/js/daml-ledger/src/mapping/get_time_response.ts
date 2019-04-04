// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const GetTimeResponse: mapping.Mapping<grpc.testing.GetTimeResponse, ledger.GetTimeResponse> = {
    toObject(message: grpc.testing.GetTimeResponse): ledger.GetTimeResponse {
        return {
            currentTime: mapping.Timestamp.toObject(message.getCurrentTime()!)
        };
    },
    toMessage(object: ledger.GetTimeResponse) {
        const message = new grpc.testing.GetTimeResponse();
        message.setCurrentTime(mapping.Timestamp.toMessage(object.currentTime));
        return message;
    }
}