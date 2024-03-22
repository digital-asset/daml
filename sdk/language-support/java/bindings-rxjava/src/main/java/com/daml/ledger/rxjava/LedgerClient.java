// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

/** Contains the set of services provided by a Ledger implementation */
public interface LedgerClient {

  /** @return The identifier of the Ledger connected to this {@link LedgerClient} */
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

  UserManagementClient getUserManagementClient();
}
