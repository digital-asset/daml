// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ClientUnaryCall } from "grpc";

/**
 * @interface ClientCancellableCall
 * @memberof ledger
 */
export class ClientCancellableCall {

    private readonly wrapped: ClientUnaryCall | undefined;

    private constructor(wrapped?: ClientUnaryCall) {
        this.wrapped = wrapped;
    }

    public static accept(wrapped: ClientUnaryCall) {
        return new ClientCancellableCall(wrapped);
    }

    public static readonly rejected = new ClientCancellableCall();

    /**
     * Cancel the ongoing call. Results in the call ending with a CANCELLED status,
     * unless it has already ended with some other status.
     *
     * @method cancel
     * @memberof ledger.ClientCancellableCall
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
     * @memberof ledger.ClientCancellableCall
     * @instance
     * @returns {string} The URI of the endpoint or an empty string if the call never reached the server
     */
    getPeer(): string {
        if (this.wrapped !== undefined) {
            return this.wrapped.getPeer();
        } else {
            return '';
        }
    }

}