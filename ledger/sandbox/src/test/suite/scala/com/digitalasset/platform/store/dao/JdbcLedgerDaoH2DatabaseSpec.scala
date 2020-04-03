// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import org.scalatest.{AsyncFlatSpec, Matchers}

// Aggregate all specs in a single run to not start a new database fixture for each one
final class JdbcLedgerDaoH2DatabaseSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendH2Database
    with JdbcLedgerDaoActiveContractsSpec
    with JdbcLedgerDaoCompletionsSpec
    with JdbcLedgerDaoConfigurationSpec
    with JdbcLedgerDaoContractKeysSpec
    with JdbcLedgerDaoContractsSpec
    with JdbcLedgerDaoDivulgenceSpec
    with JdbcLedgerDaoFetchSerializationSpec
    with JdbcLedgerDaoLedgerEntriesSpec
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoPartiesSpec
    with JdbcLedgerDaoTransactionsSpec
    with JdbcLedgerDaoTransactionTreesSpec
