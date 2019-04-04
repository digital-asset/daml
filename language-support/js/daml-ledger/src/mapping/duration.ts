// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as protobuf from 'google-protobuf/google/protobuf/duration_pb';

export const Duration: mapping.Mapping<protobuf.Duration, ledger.Duration> = {
    toObject(message: protobuf.Duration): ledger.Duration {
        return {
            seconds: message.getSeconds(),
            nanoseconds: message.getNanos()
        };
    },
    toMessage(object: ledger.Duration): protobuf.Duration {
        const message = new protobuf.Duration();
        message.setSeconds(object.seconds);
        message.setNanos(object.nanoseconds);
        return message;
    }
}