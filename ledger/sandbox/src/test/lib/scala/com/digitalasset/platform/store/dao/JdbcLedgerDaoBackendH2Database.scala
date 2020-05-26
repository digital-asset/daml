// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.platform.store.DbType
import org.scalatest.Suite

private[dao] trait JdbcLedgerDaoBackendH2Database extends JdbcLedgerDaoBackend { this: Suite =>

  override protected val jdbcUrl = "jdbc:h2:mem:static_time;db_close_delay=-1"
  override protected val dbType = DbType.H2Database

}
