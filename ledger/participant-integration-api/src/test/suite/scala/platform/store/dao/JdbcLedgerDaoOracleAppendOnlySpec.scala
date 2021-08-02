// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class JdbcLedgerDaoOracleAppendOnlySpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendOracleAppendOnly
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoActiveContractsSpec
    with JdbcLedgerDaoCompletionsSpec
    with JdbcLedgerDaoConfigurationSpec
    with JdbcLedgerDaoConfigurationAppendOnlySpec
    with JdbcLedgerDaoContractsAppendOnlySpec
    with JdbcLedgerDaoDivulgenceSpec
    with JdbcLedgerDaoExceptionSpec
    with JdbcLedgerDaoPartiesSpec
    with JdbcLedgerDaoTransactionsSpec
    with JdbcLedgerDaoContractsSpec
    with JdbcLedgerDaoTransactionTreesSpec
    with JdbcLedgerDaoContractEventsStreamSpec
    with JdbcLedgerDaoTransactionsWriterSpec
    with JdbcLedgerDaoTransactionLogUpdatesSpec
    with JdbcAppendOnlyTransactionInsertion
