// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';

import { Callback } from '../util';
import { ClientCancellableCall } from '../call/client_cancellable_call';
import { forward } from '../util/callback';

/**
 * Allows clients to verify that the server they are communicating with
 * exposes the ledger they wish to operate on.
 *
 * Note that every ledger has a unique id.
 *
 * @class LedgerIdentityClient
 * @memberof ledger
 * @param {grpc.ILedgerIdentityClient} client 
 */
export class LedgerIdentityClient {

    private static request = new grpc.GetLedgerIdentityRequest();

    private readonly client: grpc.ILedgerIdentityClient

    constructor(client: grpc.ILedgerIdentityClient) {
        this.client = client;
    }

    /**
     * Clients may call this RPC to return the identifier of the ledger they
     * are connected to.
     *
     * @method getLedgerIdentity
     * @memberof ledger.LedgerIdentityClient
     * @instance
     * @param {util.Callback<ledger.GetLedgerIdentityResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    getLedgerIdentity(callback: Callback<ledger.GetLedgerIdentityResponse>): ClientCancellableCall {
        return ClientCancellableCall.accept(this.client.getLedgerIdentity(LedgerIdentityClient.request, (error, response) => {
            forward(callback, error, response, mapping.GetLedgerIdentityResponse.toObject);
        }));
    }

}
