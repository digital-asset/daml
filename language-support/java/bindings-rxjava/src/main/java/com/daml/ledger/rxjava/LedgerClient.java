// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava;

/** Contains the set of services provided by a Ledger implementation */
public interface LedgerClient {

  /** @return The identifier of the Ledger connected to this {@link LedgerClient} */
  StateClient getStateClient();

  UpdateClient getTransactionsClient();

  CommandClient getCommandClient();

  CommandCompletionClient getCommandCompletionClient();

  CommandSubmissionClient getCommandSubmissionClient();

  EventQueryClient getEventQueryClient();

  PackageClient getPackageClient();

  TimeClient getTimeClient();

  UserManagementClient getUserManagementClient();
}
