// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';
import * as protobuf from 'google-protobuf/google/protobuf/any_pb';

export const Status: mapping.Mapping<grpc.Status, ledger.Status> = {
    toObject(message: grpc.Status): ledger.Status {
        return {
            code: message.getCode(),
            message: message.getMessage(),
            details: message.getDetailsList().map((a: protobuf.Any) => mapping.Any.toObject(a))
        };
    },
    toMessage(object: ledger.Status): grpc.Status {
        const message = new grpc.Status();
        message.setCode(object.code);
        message.setMessage(object.message);
        message.setDetailsList(object.details.map((a: ledger.Any) => mapping.Any.toMessage(a)));
        return message;
    }
}