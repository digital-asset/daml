// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Readable, ReadableOptions } from "stream";
import { Mapping } from "../mapping";
import { ClientReadableStream } from "grpc";
import { MappingTransform } from "./mapping_transform";

/**
 * A stream of objects that are pushed from the server and are readable from
 * the client. It it a {@link external:Readble} Node.js stream and wraps the
 * call to gRPC.
 *
 * @interface ClientReadableObjectStream
 * @memberof ledger
 */
export class ClientReadableObjectStream<O> extends Readable {

    private readonly wrapped?: ClientReadableStream<any>
    private readonly mapped?: MappingTransform<any, O>

    private constructor(wrapped: Error | ClientReadableStream<any>, mapping?: Mapping<any, O>, opts?: ReadableOptions) {
        super(Object.assign(opts || {}, { objectMode: true }));
        if (!(wrapped instanceof Error) && mapping !== undefined) {
            this.wrapped = wrapped;
            this.mapped = this.wrapped.pipe(new MappingTransform(mapping));
            this.mapped.on('readable', () => this._read());
            this.mapped.on('finish', () => this.emit('end'));
        } else if (wrapped instanceof Error) {
            process.nextTick(() => {
                this.emit('error', wrapped);
                process.nextTick(() => {
                    this.emit('end');
                });
            });
        }
    }

    static from<O>(wrapped: Error): ClientReadableObjectStream<O>
    static from<T, O>(wrapped: ClientReadableStream<T>, mapping: Mapping<T, O>, opts?: ReadableOptions): ClientReadableObjectStream<O>
    static from<O>(wrapped: Error | ClientReadableStream<any>, mapping?: Mapping<any, O>, opts?: ReadableOptions): ClientReadableObjectStream<O>{
        if (wrapped instanceof Error) {
            return new ClientReadableObjectStream<O>(wrapped);
        } else {
            return new ClientReadableObjectStream<O>(wrapped, mapping, opts);
        }
    }

    _read(): void {
        if (this.mapped) {
            const object = this.mapped.read();
            if (object) {
                this.push(object);
            }
        }
    }

    /**
     * Cancel the ongoing call. Results in the call ending with a CANCELLED status,
     * unless it has already ended with some other status.
     *
     * @method cancel
     * @memberof ledger.ClientReadableObjectStream
     * @instance
     */
    cancel(): void {
        if (this.wrapped) {
            this.wrapped.cancel();
        }
    }

    /**
     * Get the endpoint this call/stream is connected to.
     *
     * @method getPeer
     * @memberof ledger.ClientReadableObjectStream
     * @returns {string} The URI of the endpoint
     * @instance
     */
    getPeer(): string {
        if (this.wrapped) {
            return this.wrapped.getPeer();
        } else {
            return '';
        }
    }

}