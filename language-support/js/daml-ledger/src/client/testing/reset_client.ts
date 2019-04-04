// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import { ClientCancellableCall } from '../../call';
import { Callback } from '../../util';
import { justForward } from '../../util/callback';

export class ResetClient {

    private readonly request: grpc.testing.ResetRequest
    private readonly client: grpc.testing.IResetClient

    constructor(ledgerId: string, client: grpc.testing.IResetClient) {
        this.client = client;
        this.request = new grpc.testing.ResetRequest();
        this.request.setLedgerId(ledgerId);
    }

    reset(callback: Callback<null>) {
        return ClientCancellableCall.accept(this.client.reset(this.request, (error, _) => {
            justForward(callback, error, null)
        }));
    }

}