// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.platform.store.DbType
import org.scalatest.AsyncTestSuite

private[dao] trait JdbcLedgerDaoBackendH2AppendOnly extends JdbcLedgerDaoBackend {
  this: AsyncTestSuite =>

  override protected val dbType: DbType = DbType.H2Database

  override protected val jdbcUrl: String = "jdbc:h2:mem:static_time;db_close_delay=-1"

  override protected val enableAppendOnlySchema: Boolean = true
}
