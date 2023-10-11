// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.platform.store.DbType
import org.scalatest.AsyncTestSuite

private[dao] trait JdbcLedgerDaoBackendH2Database extends JdbcLedgerDaoBackend {
  this: AsyncTestSuite =>

  override protected val dbType: DbType = DbType.H2Database

  override protected val jdbcUrl: String =
    s"jdbc:h2:mem:${getClass.getSimpleName.toLowerCase};db_close_delay=-1"
}
