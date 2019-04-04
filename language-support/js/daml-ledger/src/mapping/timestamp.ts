// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as protobuf from 'google-protobuf/google/protobuf/timestamp_pb';

export const Timestamp: mapping.Mapping<protobuf.Timestamp, ledger.Timestamp> = {
    toObject(timestamp: protobuf.Timestamp): ledger.Timestamp {
        return {
            seconds: timestamp.getSeconds(),
            nanoseconds: timestamp.getNanos()
        }
    },
    toMessage(timestamp: ledger.Timestamp): protobuf.Timestamp {
        const result = new protobuf.Timestamp();
        result.setSeconds(timestamp.seconds);
        result.setNanos(timestamp.nanoseconds);
        return result;
    }
}