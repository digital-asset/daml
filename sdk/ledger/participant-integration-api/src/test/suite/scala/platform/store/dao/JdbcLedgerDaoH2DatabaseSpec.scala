// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

// Aggregate all specs in a single run to not start a new database fixture for each one
final class JdbcLedgerDaoH2DatabaseSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendH2Database
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoActiveContractsSpec
    with JdbcLedgerDaoCompletionsSpec
    with JdbcLedgerDaoConfigurationSpec
    with JdbcLedgerDaoContractsSpec
    with JdbcLedgerDaoDivulgenceSpec
    with JdbcLedgerDaoExceptionSpec
    with JdbcLedgerDaoPartiesSpec
    with JdbcLedgerDaoTransactionsSpec
    with JdbcLedgerDaoTransactionTreesSpec
    with JdbcLedgerDaoContractEventsStreamSpec
    with JdbcLedgerDaoTransactionsWriterSpec
    with JdbcLedgerDaoTransactionLogUpdatesSpec
