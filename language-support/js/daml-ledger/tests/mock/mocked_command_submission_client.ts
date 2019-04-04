// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as sinon from 'sinon';
import { MockedClientUnaryCall } from './client_unary_call';
import { Metadata, CallOptions, ClientUnaryCall } from 'grpc';
import { Empty } from 'google-protobuf/google/protobuf/empty_pb';

export class MockedCommandSubmissionClient implements grpc.ICommandSubmissionClient {
    
    private readonly lastRequestSpy: sinon.SinonSpy
    private static readonly empty = new Empty();

    constructor(lastRequestSpy: sinon.SinonSpy) {
        this.lastRequestSpy = lastRequestSpy;
    }

    submit(request: grpc.SubmitRequest, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    submit(request: grpc.SubmitRequest, metadata: Metadata, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    submit(request: grpc.SubmitRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    submit(request: any, metadata: any, options?: any, callback?: any) {
        this.lastRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback;
        cb(null, MockedCommandSubmissionClient.empty);
        return MockedClientUnaryCall.Instance;
    }

}