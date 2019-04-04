// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';

import { Callback } from '../util';
import { ClientCancellableCall } from '../call/client_cancellable_call';
import { forward } from '../util/callback';

/**
 * Allows clients to query the DAML LF packages that are supported by the
 * server.
 *
 * @class PackageClient
 * @memberof ledger
 * @param {string} ledgerId
 * @param {grpc.IPackageClient} client
 */
export class PackageClient {

    private ledgerId: string;
    private listPackagesRequest: grpc.ListPackagesRequest
    private client: grpc.IPackageClient;

    constructor(ledgerId: string, client: grpc.IPackageClient) {
        this.client = client;
        this.ledgerId = ledgerId;
        this.listPackagesRequest = new grpc.ListPackagesRequest();
        this.listPackagesRequest.setLedgerId(this.ledgerId);
    }

    /**
     * Returns the identifiers of all supported packages.
     *
     * @method listPackages
     * @memberof ledger.PackageClient
     * @instance
     * @param {util.Callback<ledger.ListPackagesResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    listPackages(callback: Callback<ledger.ListPackagesResponse>): ClientCancellableCall {
        return ClientCancellableCall.accept(this.client.listPackages(this.listPackagesRequest, (error, response) => {
            forward(callback, error, response, mapping.ListPackagesResponse.toObject);
        }));
    }

    /**
     * Returns the contents of a single package, or a NOT_FOUND error if the
     * requested package is unknown.
     *
     * @method getPackage
     * @memberof ledger.PackageClient
     * @instance
     * @param {string} packageId
     * @param {util.Callback<ledger.GetPackageResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    getPackage(packageId: string, callback: Callback<ledger.GetPackageResponse>): ClientCancellableCall {
        const request = new grpc.GetPackageRequest();
        request.setLedgerId(this.ledgerId);
        request.setPackageId(packageId);
        return ClientCancellableCall.accept(this.client.getPackage(request, (error, response) => {
            forward(callback, error, response, mapping.GetPackageResponse.toObject);
        }));
    }

    /**
     * Returns the status of a single package.
     *
     * @method getPackageStatus
     * @memberof ledger.PackageClient
     * @instance
     * @param {string} packageId
     * @param {util.Callback<ledger.GetPackageStatusResponse>} callback
     * @returns {ledger.ClientCancellableCall}
     */
    getPackageStatus(packageId: string, callback: Callback<ledger.GetPackageStatusResponse>): ClientCancellableCall {
        const request = new grpc.GetPackageStatusRequest();
        request.setLedgerId(this.ledgerId);
        request.setPackageId(packageId);
        return ClientCancellableCall.accept(this.client.getPackageStatus(request, (error, response) => {
            forward(callback, error, response, mapping.GetPackageStatusResponse.toObject);
        }));
    }

}
