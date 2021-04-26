// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.platform.store.DbType
import com.daml.testing.oracle.OracleAroundAll
import org.scalatest.AsyncTestSuite

private[dao] trait JdbcLedgerDaoBackendOracle extends JdbcLedgerDaoBackend with OracleAroundAll {
  this: AsyncTestSuite =>

  override protected val dbType: DbType = DbType.Oracle

  //TODO BH: figure out why username is not being picked up
  override protected def jdbcUrl: String =
    s"jdbc:oracle:thin:$oracleUser/$oraclePwd@//localhost:$oraclePort/ORCLPDB1"
}
