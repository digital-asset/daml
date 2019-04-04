// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as mapping from '.';
import * as ledger from '..';
import * as protobuf from 'google-protobuf/google/protobuf/any_pb';

export const Any: mapping.Mapping<protobuf.Any, ledger.Any> = {
    toObject(message: protobuf.Any): ledger.Any {
        return {
            value: message.getValue_asB64(),
            typeUrl: message.getTypeUrl()
        }
    },
    toMessage(object: ledger.Any): protobuf.Any {
        const message = new protobuf.Any();
        message.setValue(object.value);
        message.setTypeUrl(object.typeUrl);
        return message;
    }
}