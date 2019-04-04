// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'grpc';
import { Readable } from 'stream';

export class MockedClientReadableStream<T> extends Readable {

    public static with<T>(items: Iterable<T>): grpc.ClientReadableStream<T> {
        return new MockedClientReadableStream(items) as grpc.ClientReadableStream<T>;
    }

    private readonly iterator: Iterator<T>;

    private constructor(items: Iterable<T>) {
        super({ objectMode: true });
        this.iterator = items[Symbol.iterator]();
    }

    _read(): void {
        const next = this.iterator.next();
        if (next.done) {
          this.push(null);
        } else {
          this.push(next.value);
        }
    }

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
