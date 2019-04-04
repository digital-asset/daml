// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ClientUnaryCall, Metadata, CallOptions } from 'grpc';

import * as grpc from 'daml-grpc';
import { MockedClientUnaryCall } from './client_unary_call';

import * as sinon from 'sinon';

import { ClientReadableStream } from 'grpc';
import { Empty } from 'google-protobuf/google/protobuf/empty_pb';
import { MockedClientReadableStream } from './client_readable_stream';

export class MockedTimeClient implements grpc.testing.ITimeClient {

    private static readonly empty = new Empty();

    private responses: grpc.testing.GetTimeResponse[]
    private readonly latestRequestSpy: sinon.SinonSpy

    constructor(responses: grpc.testing.GetTimeResponse[], latestRequestSpy: sinon.SinonSpy) {
        this.responses = responses;
        this.latestRequestSpy = latestRequestSpy;
    }

    getTime(request: grpc.testing.GetTimeRequest, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.testing.GetTimeResponse>;
    getTime(request: grpc.testing.GetTimeRequest, metadata?: Metadata | undefined, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.testing.GetTimeResponse>;
    getTime(request: grpc.testing.GetTimeRequest, _metadata?: any, _options?: any) {
        this.latestRequestSpy(request);
        return MockedClientReadableStream.with(this.responses);
    }

    setTime(request: grpc.testing.SetTimeRequest, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    setTime(request: grpc.testing.SetTimeRequest, metadata: Metadata, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    setTime(request: grpc.testing.SetTimeRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: Empty) => void): ClientUnaryCall;
    setTime(request: grpc.testing.SetTimeRequest, metadata: any, options?: any, callback?: any) {
        this.latestRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback; 
        cb(null, MockedTimeClient.empty);
        return MockedClientUnaryCall.Instance;
    }



}