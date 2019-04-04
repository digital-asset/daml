// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ClientUnaryCall, Metadata, CallOptions } from 'grpc';

import * as grpc from 'daml-grpc';
import { MockedClientUnaryCall } from './client_unary_call';

import * as sinon from 'sinon';

import { Empty } from 'google-protobuf/google/protobuf/empty_pb';

export class MockedResetClient implements grpc.testing.IResetClient {

    private static readonly empty = new Empty();

    private readonly latestRequestSpy: sinon.SinonSpy

    constructor(latestRequestSpy: sinon.SinonSpy) {
        this.latestRequestSpy = latestRequestSpy;
    }

    reset(request: grpc.testing.ResetRequest, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    reset(request: grpc.testing.ResetRequest, metadata: Metadata, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    reset(request: grpc.testing.ResetRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    reset(request: grpc.testing.ResetRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback; 
        cb(null, MockedResetClient.empty);
        return MockedClientUnaryCall.Instance;
    }



}