// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

/**
 * Contains the set of services provided by a Ledger implementation
 */
public interface LedgerClient {

    /**
     * @return The identifier of the Ledger connected to this {@link LedgerClient}
     */
    String getLedgerId();

    ActiveContractsClient getActiveContractSetClient();

    TransactionsClient getTransactionsClient();

    CommandClient getCommandClient();

    CommandCompletionClient getCommandCompletionClient();

    CommandSubmissionClient getCommandSubmissionClient();

    LedgerIdentityClient getLedgerIdentityClient();

    PackageClient getPackageClient();

    LedgerConfigurationClient getLedgerConfigurationClient();

    TimeClient getTimeClient();
}
