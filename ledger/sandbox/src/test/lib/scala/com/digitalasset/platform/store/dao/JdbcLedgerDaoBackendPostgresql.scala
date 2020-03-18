// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import com.digitalasset.platform.store.DbType
import com.digitalasset.testing.postgresql.PostgresAroundAll
import org.scalatest.Suite

private[dao] trait JdbcLedgerDaoBackendPostgresql
    extends JdbcLedgerDaoBackend
    with PostgresAroundAll { this: Suite =>

  override protected val dbType: DbType = DbType.Postgres
  override protected def jdbcUrl: String = postgresFixture.jdbcUrl

}
