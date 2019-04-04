// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ClientUnaryCall, Metadata, CallOptions } from 'grpc';

import * as grpc from 'daml-grpc';
import { MockedClientUnaryCall } from './client_unary_call';

export class MockedLedgerIdentityClient implements grpc.ILedgerIdentityClient {

    private readonly ledgerId: grpc.GetLedgerIdentityResponse;

    constructor(ledgerId: string) {
        this.ledgerId = new grpc.GetLedgerIdentityResponse();
        this.ledgerId.setLedgerId(ledgerId);
    }
    
    getLedgerIdentity(request: grpc.GetLedgerIdentityRequest, callback: (error: Error | null, response: grpc.GetLedgerIdentityResponse) => void): ClientUnaryCall;
    getLedgerIdentity(request: grpc.GetLedgerIdentityRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.GetLedgerIdentityResponse) => void): ClientUnaryCall;
    getLedgerIdentity(request: grpc.GetLedgerIdentityRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.GetLedgerIdentityResponse) => void): ClientUnaryCall;
    getLedgerIdentity(_request: grpc.GetLedgerIdentityRequest, metadata: any, options?: any, callback?: any) {
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback;
        cb(null, this.ledgerId);
        return MockedClientUnaryCall.Instance;
    }

}