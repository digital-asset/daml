// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'grpc';

export class MockedClientUnaryCall {

    private constructor() {
        // Do nothing, constructor is private to prevent instantiation outside of this class
    }

    public static Instance: grpc.ClientUnaryCall = new MockedClientUnaryCall() as grpc.ClientUnaryCall;

    /**
     * HACK! Leverage dynamic structural typing to mock non-extensible `ClientReadableStream`s
     * 
     * Cancel the ongoing call. Results in the call ending with a CANCELLED status,
     * unless it has already ended with some other status.
     */
    cancel(): void {
        throw new Error('MOCK!');
    }

    /**
     * HACK! Leverage dynamic structural typing to mock non-extensible `ClientReadableStream`s
     * 
     * Get the endpoint this call/stream is connected to.
     * @return The URI of the endpoint
     */
    getPeer(): string {
        throw new Error('MOCK!');
    }

}
