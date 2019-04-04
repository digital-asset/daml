// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';
import * as reporting from '../reporting';
import * as validation from '../validation';

import { ClientCancellableCall } from '../call/client_cancellable_call';
import { Callback } from '../util';
import { justForward } from '../util/callback';

/**
 * Command Service is able to correlate submitted commands with completion
 * ledger, identify timeouts, and return contextual information with each
 * tracking result. This supports the implementation of stateless clients.
 *
 * @class CommandClient
 * @memberof ledger
 * @param {string} ledgerId 
 * @param {grpc.ICommandClient} client 
 */
export class CommandClient {

    private readonly ledgerId: string
    private readonly client: grpc.ICommandClient
    private readonly reporter: reporting.Reporter

    constructor(ledgerId: string, client: grpc.ICommandClient, reporter: reporting.Reporter) {
        this.ledgerId = ledgerId;
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Submits a single composite command and waits for its result.
     *
     * Returns RESOURCE_EXHAUSTED if the number of in-flight commands reached
     * the maximum (if a limit is configured).
     *
     * Propagates the gRPC error of failed submissions including DAML
     * interpretation errors.
     *
     * @method submitAndWait
     * @memberof ledger.CommandClient
     * @instance
     * @param {ledger.SubmitAndWaitRequest} requestObject
     * @param {util.Callback<null>} callback 
     * @returns {ledger.ClientCancellableCall}
     */
    submitAndWait(requestObject: ledger.SubmitAndWaitRequest, callback: Callback<null>): ClientCancellableCall {
        const tree = validation.SubmitAndWaitRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.SubmitAndWaitRequest.toMessage(requestObject);
            if (request.hasCommands()) {
                request.getCommands()!.setLedgerId(this.ledgerId);
            }
            return ClientCancellableCall.accept(this.client.submitAndWait(request, (error, _) => {
                justForward(callback, error, null)
            }));
        } else {
            setImmediate(() => callback(this.reporter(tree)));
            return ClientCancellableCall.rejected;
        }
    }

}