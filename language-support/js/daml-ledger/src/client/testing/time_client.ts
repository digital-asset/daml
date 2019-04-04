// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as grpc from 'daml-grpc';
import * as ledger from '../..';
import * as mapping from '../../mapping';
import * as reporting from '../../reporting';
import * as validation from '../../validation';
import { ClientReadableObjectStream, ClientCancellableCall } from '../../call';
import { Callback } from '../../util';
import { justForward } from '../../util/callback';

/**
 * Optional service, exposed for testing static time scenarios.
 *
 * @class TimeClient
 * @memberof ledger.testing
 * @param {string} ledgerId
 * @param {grpc.testing.ITimeClient} client
 */
export class TimeClient {

    private readonly ledgerId: string
    private readonly client: grpc.testing.ITimeClient
    private readonly reporter: reporting.Reporter;

    constructor(ledgerId: string, client: grpc.testing.ITimeClient, reporter: reporting.Reporter) {
        this.ledgerId = ledgerId;
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Returns a stream of time updates.
     *
     * Always returns at least one response, where the first one is the current
     * time.
     *
     * Subsequent responses are emitted whenever the ledger server's time is
     * updated.
     *
     * @method getTime
     * @memberof ledger.testing.TimeClient
     * @instance
     * @returns {ledger.ClientReadableObjectStream<ledger.GetTimeResponse>}
     */
    getTime(): ClientReadableObjectStream<ledger.GetTimeResponse> {
        const request = new grpc.testing.GetTimeRequest();
        request.setLedgerId(this.ledgerId);
        return ClientReadableObjectStream.from(this.client.getTime(request), mapping.GetTimeResponse);
    }

    /**
     * Allows clients to change the ledger's clock in an atomic get-and-set
     * operation.
     *
     * @method setTime
     * @memberof ledger.testing.TimeClient
     * @instance
     * @param {ledger.SetTimeRequest} requestObject
     * @param {util.Callback<null>} callback
     */
    setTime(requestObject: ledger.SetTimeRequest, callback: Callback<null>): ClientCancellableCall {
        const tree = validation.SetTimeRequest.validate(requestObject);
        if (validation.ok(tree)) {
            const request = mapping.SetTimeRequest.toMessage(requestObject);
            request.setLedgerId(this.ledgerId);
            return ClientCancellableCall.accept(this.client.setTime(request, (error, _) => {
                justForward(callback, error, null)
            }));
        } else {
            setImmediate(() => { callback(this.reporter(tree)); });
            return ClientCancellableCall.rejected;
        }
    }

}