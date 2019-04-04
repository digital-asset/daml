// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ledger from ".";

export namespace LedgerClient {

    /**
     * The ledger required to connect to a {@link ledger.LedgerClient}
     *
     * @interface Options
     * @memberof ledger.LedgerClient
     */
    export interface Options {
        /**
         * @member {string} host
         * @memberof ledger.LedgerClient.Options
         * @instance
         */
        host: string
        /**
         * @member {number} port
         * @memberof ledger.LedgerClient.Options
         * @instance
         */
        port: number
        /**
         * @member {ledger.reporting.Reporter} reporter
         * @memberof ledger.LedgerClient.Options
         * @instance
         */
        reporter?: ledger.reporting.Reporter
        /**
         * @member {Buffer} rootCerts The root certificate ledger
         * @memberof ledger.LedgerClient.Options
         * @instance
         */
        rootCerts?: Buffer
        /**
         * @member {Buffer} privateKey The client certificate private key
         * @memberof ledger.LedgerClient.Options
         * @instance
         */
        privateKey?: Buffer
        /**
         * @member {Buffer} certChain The client certificate cert chain
         * @memberof ledger.LedgerClient.Options
         * @instance
         */
        certChain?: Buffer
    }

}

/**
 * Contains the set of services provided by a ledger implementation
 *
 * @interface LedgerClient
 * @memberof ledger
 */
export interface LedgerClient {
    /**
     * The identifier of the ledger connected to this {@link LedgerClient}
     *
     * @member {string} ledgerId
     * @memberof ledger.LedgerClient
     * @instance
     */
    ledgerId: string
    /**
     * @member {ledger.ActiveContractsClient} activeContractsClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    activeContractsClient: ledger.ActiveContractsClient
    /**
     * @member {ledger.CommandClient} commandClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    commandClient: ledger.CommandClient
    /**
     * @member {ledger.CommandCompletionClient} commandCompletionClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    commandCompletionClient: ledger.CommandCompletionClient
    /**
     * @member {ledger.CommandSubmissionClient} commandSubmissionClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    commandSubmissionClient: ledger.CommandSubmissionClient
    /**
     * @member {ledger.LedgerIdentityClient} ledgerIdentityClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    ledgerIdentityClient: ledger.LedgerIdentityClient
    /**
     * @member {ledger.PackageClient} packageClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    packageClient: ledger.PackageClient
    /**
     * @member {ledger.LedgerConfigurationClient} ledgerConfigurationClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    ledgerConfigurationClient: ledger.LedgerConfigurationClient
    /**
     * @member {ledger.testing.TimeClient} timeClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    timeClient: ledger.testing.TimeClient
    /**
     * @member {ledger.TransactionClient} transactionClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    transactionClient: ledger.TransactionClient
    /**
     * @member {ledger.testing.ResetClient} resetClient
     * @memberof ledger.LedgerClient
     * @instance
     */
    resetClient: ledger.testing.ResetClient
}