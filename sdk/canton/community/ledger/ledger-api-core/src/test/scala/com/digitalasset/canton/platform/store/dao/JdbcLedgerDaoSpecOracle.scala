// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class JdbcLedgerDaoSpecOracle
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendOracle
    with JdbcLedgerDaoActiveContractsSpec
    with JdbcLedgerDaoCompletionsSpec
    with JdbcLedgerDaoContractsSpec
    with JdbcLedgerDaoExceptionSpec
    with JdbcLedgerDaoPartiesSpec
    with JdbcLedgerDaoTransactionsSpec
    with JdbcLedgerDaoTransactionTreesSpec
    with JdbcLedgerDaoTransactionsWriterSpec
