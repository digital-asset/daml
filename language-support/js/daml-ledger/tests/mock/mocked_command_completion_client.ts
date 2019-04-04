// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as sinon from 'sinon';
import { MockedClientReadableStream } from './client_readable_stream';
import { MockedClientUnaryCall } from './client_unary_call';
import { Metadata, CallOptions, ClientReadableStream, ClientUnaryCall } from 'grpc';

export class MockedCommandCompletionClient implements grpc.ICommandCompletionClient {

    private readonly end: grpc.CompletionEndResponse
    private readonly completions: grpc.CompletionStreamResponse[]
    private readonly lastRequestSpy: sinon.SinonSpy

    constructor(completions: grpc.CompletionStreamResponse[], end: grpc.CompletionEndResponse, lastRequestSpy: sinon.SinonSpy) {
        this.end = end;
        this.completions = completions;
        this.lastRequestSpy = lastRequestSpy;
    }

    completionEnd(request: grpc.CompletionEndRequest, callback: (error: Error | null, response: grpc.CompletionEndResponse) => void): ClientUnaryCall;
    completionEnd(request: grpc.CompletionEndRequest, metadata: Metadata, callback: (error: Error | null, response: grpc.CompletionEndResponse) => void): ClientUnaryCall;
    completionEnd(request: grpc.CompletionEndRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: Error | null, response: grpc.CompletionEndResponse) => void): ClientUnaryCall;
    completionEnd(request: grpc.CompletionEndRequest, metadata: any, options?: any, callback?: any) {
        this.lastRequestSpy(request);
        const cb = callback === undefined ? (options === undefined ? metadata : options ) : callback;
        cb(null, this.end);
        return MockedClientUnaryCall.Instance;
    }

    completionStream(request: grpc.CompletionStreamRequest, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.CompletionStreamResponse>;    completionStream(request: grpc.CompletionStreamRequest, metadata?: Metadata | undefined, options?: Partial<CallOptions> | undefined): ClientReadableStream<grpc.CompletionStreamResponse>;
    completionStream(request: grpc.CompletionStreamRequest, _metadata?: any, _options?: any) {
        this.lastRequestSpy(request);
        return MockedClientReadableStream.with(this.completions);
    }

}