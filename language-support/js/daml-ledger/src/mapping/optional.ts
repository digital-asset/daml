// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as grpc from 'daml-grpc';

export const Optional: mapping.Mapping<grpc.Optional, ledger.Optional> = {
    toObject(message: grpc.Optional): ledger.Optional {
        const object: ledger.Optional = {};
        if (message.hasValue()) {
            object.value = mapping.Value.toObject(message.getValue()!);
        }
        return object;
    },
    toMessage(object: ledger.Optional): grpc.Optional {
        const message = new grpc.Optional();
        if (object.value !== undefined) {
            message.setValue(mapping.Value.toMessage(object.value));
        }
        return message;
    }
};