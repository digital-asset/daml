// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ChannelCredentials, credentials } from 'grpc';

import * as ledger from '.';
import * as grpc from 'daml-grpc';

import { Callback, forward } from './util/callback';

/**
 * A {@link ledger.LedgerClient} implementation that connects to an existing Ledger and provides clients to query it. To use the {@link ledger.DamlLedgerClient}
 * call the static `connect` method, passing an instance of {@link ledger.Options} with the host, port and (for secure connection) the
 * necessary certificates.
 *
 * @interface DamlLedgerClient
 * @memberof ledger
 * @implements ledger.LedgerClient
 */
export class DamlLedgerClient implements ledger.LedgerClient {

    /**
     * Connects a new instance of the {@link ledger.DamlLedgerClient} to the Ledger.
     * 
     * @method connect
     * @memberof ledger.DamlLedgerClient
     * @param {ledger.LedgerClient.Options} options The host, port and certificates needed to reach the ledger
     * @param {util.Callback<ledger.LedgerClient>} callback A callback that will be either passed an error or the LedgerClient instance in case of successfull connection
     */
    static connect(options: ledger.LedgerClient.Options, callback: Callback<ledger.LedgerClient>): void {

        let creds: ChannelCredentials;
        if (!options.certChain && !options.privateKey && !options.rootCerts) {
            creds = credentials.createInsecure();
        } else if (options.certChain && options.privateKey && options.rootCerts) {
            creds = credentials.createSsl(options.rootCerts, options.privateKey, options.certChain)
        } else {
            setImmediate(() => {
                callback(new Error(`Incomplete information provided to establish a secure connection (certChain: ${!!options.certChain}, privateKey: ${!!options.privateKey}, rootCerts: ${!!options.rootCerts})`));
            });
            return;
        }

        const reporter = options.reporter || ledger.reporting.SimpleReporter;
        const address = `${options.host}:${options.port}`;
        const client = new grpc.LedgerIdentityClient(address, creds);

        client.getLedgerIdentity(new grpc.GetLedgerIdentityRequest(), (error, response) => {
            forward(callback, error, response, (response) => {
                return new DamlLedgerClient(response.getLedgerId(), address, creds, reporter);
            });
        });

    }

    public readonly ledgerId: string;

    private readonly _activeContractsClient: ledger.ActiveContractsClient;
    private readonly _commandClient: ledger.CommandClient;
    private readonly _commandCompletionClient: ledger.CommandCompletionClient;
    private readonly _commandSubmissionClient: ledger.CommandSubmissionClient;
    private readonly _ledgerIdentityClient: ledger.LedgerIdentityClient;
    private readonly _packageClient: ledger.PackageClient;
    private readonly _ledgerConfigurationClient: ledger.LedgerConfigurationClient;
    private readonly _timeClient: ledger.testing.TimeClient;
    private readonly _transactionClient: ledger.TransactionClient;
    private readonly _resetClient: ledger.testing.ResetClient;

    private constructor(ledgerId: string, address: string, creds: ChannelCredentials, reporter: ledger.reporting.Reporter) {
        this.ledgerId = ledgerId;
        this._activeContractsClient =
            new ledger.ActiveContractsClient(ledgerId, new grpc.ActiveContractsClient(address, creds), reporter);
        this._commandClient =
            new ledger.CommandClient(ledgerId, new grpc.CommandClient(address, creds), reporter);
        this._commandCompletionClient =
            new ledger.CommandCompletionClient(ledgerId, new grpc.CommandCompletionClient(address, creds), reporter);
        this._commandSubmissionClient =
            new ledger.CommandSubmissionClient(ledgerId, new grpc.CommandSubmissionClient(address, creds), reporter);
        this._ledgerIdentityClient =
            new ledger.LedgerIdentityClient(new grpc.LedgerIdentityClient(address, creds));
        this._packageClient =
            new ledger.PackageClient(ledgerId, new grpc.PackageClient(address, creds));
        this._ledgerConfigurationClient =
            new ledger.LedgerConfigurationClient(ledgerId, new grpc.LedgerConfigurationClient(address, creds));
        this._timeClient =
            new ledger.testing.TimeClient(ledgerId, new grpc.testing.TimeClient(address, creds), reporter);
        this._transactionClient =
            new ledger.TransactionClient(ledgerId, new grpc.TransactionClient(address, creds), reporter);
        this._resetClient =
            new ledger.testing.ResetClient(ledgerId, new grpc.testing.ResetClient(address, creds));
    }

    get activeContractsClient(): ledger.ActiveContractsClient {
        return this._activeContractsClient;
    }

    get commandClient(): ledger.CommandClient {
        return this._commandClient;
    }

    get commandCompletionClient(): ledger.CommandCompletionClient {
        return this._commandCompletionClient;
    }

    get commandSubmissionClient(): ledger.CommandSubmissionClient {
        return this._commandSubmissionClient;
    }

    get ledgerIdentityClient(): ledger.LedgerIdentityClient {
        return this._ledgerIdentityClient;
    }

    get packageClient(): ledger.PackageClient {
        return this._packageClient;
    }

    get ledgerConfigurationClient(): ledger.LedgerConfigurationClient {
        return this._ledgerConfigurationClient;
    }

    get timeClient(): ledger.testing.TimeClient {
        return this._timeClient;
    }

    get transactionClient(): ledger.TransactionClient {
        return this._transactionClient;
    }

    get resetClient(): ledger.testing.ResetClient {
        return this._resetClient;
    }

}