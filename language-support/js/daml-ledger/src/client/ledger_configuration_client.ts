// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '..';
import * as mapping from '../mapping';

import { ClientReadableObjectStream } from '../call/client_readable_object_stream';

/**
 * LedgerConfigurationService allows clients to subscribe to changes of
 * the ledger configuration.
 *
 * @class LedgerConfigurationClient
 * @memberof ledger
 * @param {string} ledgerId 
 * @param {grpc.ILedgerConfigurationClient} client 
 */
export class LedgerConfigurationClient {

    private readonly request: grpc.GetLedgerConfigurationRequest
    private readonly client: grpc.ILedgerConfigurationClient

    constructor(ledgerId: string, client: grpc.ILedgerConfigurationClient) {
        this.request = new grpc.GetLedgerConfigurationRequest()
        this.request.setLedgerId(ledgerId);
        this.client = client;
    }

    /**
     * GetLedgerConfiguration returns the latest configuration as the first response, and publishes configuration updates in the same stream.
     *
     * @method getLedgerConfiguration
     * @memberof ledger.LedgerConfigurationClient
     * @instance
     * @returns {ledger.ClientReadableObjectStream<ledger.GetLedgerConfigurationResponse>}
     */
    getLedgerConfiguration(): ClientReadableObjectStream<ledger.GetLedgerConfigurationResponse> {
        return ClientReadableObjectStream.from(this.client.getLedgerConfiguration(this.request), mapping.GetLedgerConfigurationResponse);
    }

}