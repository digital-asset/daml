// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class JdbcLedgerDaoOracleSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendOracle
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
    with JdbcLedgerDaoTransactionsWriterSpec
