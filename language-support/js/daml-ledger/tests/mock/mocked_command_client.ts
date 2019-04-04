// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import { MockedClientUnaryCall } from './client_unary_call';
import { Empty } from 'google-protobuf/google/protobuf/empty_pb';
import { CallOptions, ClientUnaryCall, Metadata } from 'grpc';
import { SinonSpy } from 'sinon';

export class MockedCommandClient implements grpc.ICommandClient {
    
    private readonly lastRequestSpy: SinonSpy

    constructor(lastRequestSpy: SinonSpy) {
        this.lastRequestSpy = lastRequestSpy;
    }

    submitAndWait(request: grpc.SubmitAndWaitRequest, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    submitAndWait(request: grpc.SubmitAndWaitRequest, metadata: Metadata, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    submitAndWait(request: grpc.SubmitAndWaitRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    submitAndWait(request: grpc.SubmitAndWaitRequest, metadata: any, options?: any, callback?: any): ClientUnaryCall {
        this.lastRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback;
        cb(null, Empty);
        return MockedClientUnaryCall.Instance;
    }

}