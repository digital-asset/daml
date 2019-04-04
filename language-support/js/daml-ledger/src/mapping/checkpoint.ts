// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Checkpoint: mapping.Mapping<grpc.Checkpoint, ledger.Checkpoint> = {
    toObject(message: grpc.Checkpoint): ledger.Checkpoint {
        return {
            offset: mapping.LedgerOffset.toObject(message.getOffset()!),
            recordTime: mapping.Timestamp.toObject(message.getRecordTime()!)
        };
    },
    toMessage(object: ledger.Checkpoint): grpc.Checkpoint {
        const message = new grpc.Checkpoint();
        message.setOffset(mapping.LedgerOffset.toMessage(object.offset));
        message.setRecordTime(mapping.Timestamp.toMessage(object.recordTime));
        return message;
    }
}