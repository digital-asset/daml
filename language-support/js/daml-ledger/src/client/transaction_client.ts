// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';
import * as validation from '../validation';
import * as reporting from '../reporting';

import { Callback } from '../util';
import { ClientReadableObjectStream } from '../call/client_readable_object_stream';
import { ClientCancellableCall } from '../call/client_cancellable_call';
import { forward } from '../util/callback';

/**
 * Allows clients to read transactions from the ledger.
 *
 * @class TransactionClient
 * @memberof ledger
 * @param {string} ledgerId
 * @param {grpc.ITransactionClient} client
 */
export class TransactionClient {

    private readonly ledgerId: string;
    private readonly client: grpc.ITransactionClient;
    private readonly reporter: reporting.Reporter;

    constructor(ledgerId: string, client: grpc.ITransactionClient, reporter: reporting.Reporter) {
        this.ledgerId = ledgerId;
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Get the current ledger end.
     *
     * Subscriptions started with the returned offset will serve transactions
     * created after this RPC was called.
     *
     * @method getLedgerEnd
     * @memberof ledger.TransactionClient
     * @instance
     * @param {util.Callback<ledger.GetLedgerEndResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    getLedgerEnd(callback: Callback<ledger.GetLedgerEndResponse>): ClientCancellableCall {
        const request = new grpc.GetLedgerEndRequest();
        request.setLedgerId(this.ledgerId);
        return ClientCancellableCall.accept(this.client.getLedgerEnd(request, (error, response) => {
            forward(callback, error, response, mapping.GetLedgerEndResponse.toObject);
        }));
    }

    /**
     * Lookup a transaction by the ID of an event that appears within it.
     *
     * This call is a future extension point and is currently not supported.
     *
     * Returns NOT_FOUND if no such transaction exists.
     *
     * @method getTransactionByEventId
     * @memberof ledger.TransactionClient
     * @instance
     * @param {ledger.GetTransactionByEventIdRequest} requestObject
     * @param {util.Callback<ledger.GetTransactionResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    getTransactionByEventId(requestObject: ledger.GetTransactionByEventIdRequest, callback: Callback<ledger.GetTransactionResponse>): ClientCancellableCall {
        const tree = validation.GetTransactionByEventIdRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.GetTransactionByEventIdRequest.toMessage(requestObject);
            request.setLedgerId(this.ledgerId);
            return ClientCancellableCall.accept(this.client.getTransactionByEventId(request, (error, response) => {
                forward(callback, error, response, mapping.GetTransactionResponse.toObject);
            }));
        } else {
            setImmediate(() => callback(this.reporter(tree)));
            return ClientCancellableCall.rejected;
        }
    }

    /**
     * Lookup a transaction by its ID.
     *
     * This call is a future extension point and is currently not supported.
     *
     * Returns NOT_FOUND if no such transaction exists.
     *
     * @method getTransactionById
     * @memberof ledger.TransactionClient
     * @instance
     * @param {ledger.GetTransactionByIdRequest} requestObject
     * @param {util.Callback<ledger.GetTransactionResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    getTransactionById(requestObject: ledger.GetTransactionByIdRequest, callback: Callback<ledger.GetTransactionResponse>): ClientCancellableCall {
        const request = mapping.GetTransactionByIdRequest.toMessage(requestObject);
        request.setLedgerId(this.ledgerId);
        return ClientCancellableCall.accept(this.client.getTransactionById(request, (error, response) => {
            forward(callback, error, response, mapping.GetTransactionResponse.toObject);
        }));
    }

    /**
     * Read the ledger's filtered transaction stream for a set of parties.
     *
     * @method getTransactions
     * @memberof ledger.TransactionClient
     * @instance
     * @param {ledger.GetTransactionsRequest} requestObject
     * @returns {ledger.ClientReadableObjectStream<ledger.GetTransactionsResponse>}
     */
    getTransactions(requestObject: ledger.GetTransactionsRequest): ClientReadableObjectStream<ledger.GetTransactionsResponse> {
        const tree = validation.GetTransactionsRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.GetTransactionsRequest.toMessage(requestObject);
            request.setLedgerId(this.ledgerId);
            if (requestObject.verbose === undefined) {
                request.setVerbose(true);
            }
            return ClientReadableObjectStream.from(this.client.getTransactions(request), mapping.GetTransactionsResponse);
        } else {
            return ClientReadableObjectStream.from(this.reporter(tree));
        }
    }

    /**
     * Read the ledger's complete transaction stream for a set of parties.
     *
     * This call is a future extension point and is currently not supported.
     *
     * @method getTransactionTrees
     * @memberof ledger.TransactionClient
     * @instance
     * @param {ledger.GetTransactionsRequest} requestObject
     * @returns {ledger.ClientReadableObjectStream<ledger.GetTransactionTreesResponse>}
     */
    getTransactionTrees(requestObject: ledger.GetTransactionsRequest): ClientReadableObjectStream<ledger.GetTransactionTreesResponse> {
        const tree = validation.GetTransactionsRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.GetTransactionsRequest.toMessage(requestObject);
            request.setLedgerId(this.ledgerId);
            return ClientReadableObjectStream.from(this.client.getTransactionTrees(request), mapping.GetTransactionTreesResponse);
        } else {
            return ClientReadableObjectStream.from(this.reporter(tree));
        }
    }

}
