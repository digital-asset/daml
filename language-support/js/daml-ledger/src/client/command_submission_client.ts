// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';
import * as reporting from '../reporting';
import * as validation from '../validation';

import { Callback } from '../util';
import { ClientCancellableCall } from '../call/client_cancellable_call';
import { justForward } from '../util/callback';

/**
 * Allows clients to attempt advancing the ledger's state by submitting
 * commands.
 *
 * The final states of their submissions are disclosed by the Command
 * Completion Service.
 *
 * The on-ledger effects of their submissions are disclosed by the
 * Transaction Service.
 *
 * Commands may fail in 4 distinct manners:
 *
 * 1) INVALID_PARAMETER gRPC error on malformed payloads and missing
 *    required fields.
 *
 * 2) Failure communicated in the SubmitResponse.
 *
 * 3) Failure communicated in a Completion.
 *
 * 4) A Checkoint with record_time &gt; command mrt arrives through the
 *    Completion Stream, and the command's Completion was not visible
 *    before. In this case the command is lost.
 *
 * Clients that do not receive a successful completion about their
 * submission MUST NOT assume that it was successful.
 *
 * Clients SHOULD subscribe to the CompletionStream before starting to
 * submit commands to prevent race conditions.
 *
 * Interprocess tracing of command submissions may be achieved via Zipkin
 * by filling out the trace_context field.
 *
 * The server will return a child context of the submitted one, (or a new
 * one if the context was missing) on both the Completion and Transaction
 * streams.
 *
 * @class CommandSubmissionClient
 * @memberof ledger
 * @param {string} ledgerId 
 * @param {grpc.ICommandSubmissionClient} client
 */
export class CommandSubmissionClient {

    private readonly ledgerId: string;
    private readonly client: grpc.ICommandSubmissionClient;
    private readonly reporter: reporting.Reporter;

    constructor(ledgerId: string, client: grpc.ICommandSubmissionClient, reporter: reporting.Reporter) {
        this.ledgerId = ledgerId;
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Submit a single composite command.
     *
     * @method submit
     * @memberof ledger.CommandSubmissionClient
     * @instance
     * @param {ledger.SubmitRequest} requestObject
     * @param {util.Callback<null>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    submit(requestObject: ledger.SubmitRequest, callback: Callback<null>): ClientCancellableCall {
        const tree = validation.SubmitRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.SubmitRequest.toMessage(requestObject);
            if (request.hasCommands()) {
                request.getCommands()!.setLedgerId(this.ledgerId);
            }
            return ClientCancellableCall.accept(this.client.submit(request, (error, _) => {
                justForward(callback, error, null);
            }));
        } else {
            setImmediate(() => callback(this.reporter(tree)));
            return ClientCancellableCall.rejected;
        }
    }

}
