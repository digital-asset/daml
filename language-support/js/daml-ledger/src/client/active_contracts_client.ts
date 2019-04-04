// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';
import * as reporting from '../reporting';
import * as validation from '../validation';
import { ClientReadableObjectStream } from '../call/client_readable_object_stream';

/**
 * Allows clients to initialize themselves according to a fairly recent state
 * of the ledger without reading through all transactions that were committed
 * since the ledger's creation.
 *
 * @class ActiveContractsClient
 * @memberof ledger
 * @param {string} ledgerId
 * @param {grpc.IActiveContractsClient} client
 */
export class ActiveContractsClient {

    private readonly ledgerId: string;
    private readonly client: grpc.IActiveContractsClient;
    private readonly reporter: reporting.Reporter;

    constructor(ledgerId: string, client: grpc.IActiveContractsClient, reporter: reporting.Reporter) {
        this.ledgerId = ledgerId;
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Returns a stream of the latest snapshot of active contracts. Getting an
     * empty stream means that the active contracts set is empty and the client
     * should listen to transactions using LEDGER_BEGIN.
     *
     * Clients SHOULD NOT assume that the set of active contracts they receive
     * reflects the state at the ledger end.
     *
     * @method getActiveContracts
     * @memberof ledger.ActiveContractsClient
     * @instance
     * @param {ledger.GetActiveContractsRequest} requestObject
     * @returns {ledger.ClientReadableObjectStream<ledger.GetActiveContractsResponse>}
     */
    getActiveContracts(requestObject: ledger.GetActiveContractsRequest): ClientReadableObjectStream<ledger.GetActiveContractsResponse> {
        const tree = validation.GetActiveContractsRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.GetActiveContractsRequest.toMessage(requestObject);
            request.setLedgerId(this.ledgerId);
            if (requestObject.verbose === undefined) {
                request.setVerbose(true);
            }
            return ClientReadableObjectStream.from(this.client.getActiveContracts(request), mapping.GetActiveContractsResponse);
        } else {
            return ClientReadableObjectStream.from(this.reporter(tree));
        }
    }

}