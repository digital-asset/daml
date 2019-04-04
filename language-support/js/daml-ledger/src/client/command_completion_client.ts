// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';
import * as reporting from '../reporting';
import * as validation from '../validation';
import { ClientReadableObjectStream } from '../call/client_readable_object_stream';
import { ClientCancellableCall } from '../call/client_cancellable_call';
import { Callback } from '../util';
import { forward } from '../util/callback';

/**
 * Allows clients to observe the status of their submissions.
 *
 * Commands may be submitted via the Command Submission Service.
 *
 * The on-ledger effects of their submissions are disclosed by the
 * Transaction Service.
 *
 * Commands may fail in 4 distinct manners:
 *
 * 1. INVALID_PARAMETER gRPC error on malformed payloads and missing
 *    required fields.
 *
 * 2. Failure communicated in the SubmitResponse.
 *
 * 3. Failure communicated in a Completion.
 *
 * 4. A Checkpoint with record_time &gt; command mrt arrives through the
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
 * one if the context was missing) on both the Completion and Transaction streams.
 *
 * @class CommandCompletionClient
 * @memberof ledger
 * @param {string} ledgerId 
 * @param {grpc.ICommandCompletionClient} client 
 */
export class CommandCompletionClient {

    private readonly completionEndRequest: grpc.CompletionEndRequest
    private readonly client: grpc.ICommandCompletionClient
    private readonly ledgerId: string
    private readonly reporter: reporting.Reporter

    constructor(ledgerId: string, client: grpc.ICommandCompletionClient, reporter: reporting.Reporter) {
        this.completionEndRequest = new grpc.CompletionEndRequest();
        this.completionEndRequest.setLedgerId(ledgerId);
        this.ledgerId = ledgerId;
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Subscribe to command completion events.
     *
     * @method completionStream
     * @memberof ledger.CommandCompletionClient
     * @instance
     * @param {ledger.CompletionStreamRequest} requestObject
     * @returns {ledger.ClientReadableObjectStream<ledger.CompletionStreamResponse>}
     */
    completionStream(requestObject: ledger.CompletionStreamRequest): ClientReadableObjectStream<ledger.CompletionStreamResponse> {
        const tree = validation.CompletionStreamRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.CompletionStreamRequest.toMessage(requestObject);
            request.setLedgerId(this.ledgerId);
            return ClientReadableObjectStream.from(this.client.completionStream(request), mapping.CompletionStreamResponse);
        } else {
            return ClientReadableObjectStream.from(this.reporter(tree));
        }
    }

    /**
     * Returns the offset after the latest completion.
     *
     * @method completionEnd
     * @memberof ledger.CommandCompletionClient
     * @instance
     * @param {util.Callback<ledger.CompletionEndResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    completionEnd(callback: Callback<ledger.CompletionEndResponse>): ClientCancellableCall {
        return ClientCancellableCall.accept(this.client.completionEnd(this.completionEndRequest, (error, response) => {
            forward(callback, error, response, mapping.CompletionEndResponse.toObject);
        }));
    }

}