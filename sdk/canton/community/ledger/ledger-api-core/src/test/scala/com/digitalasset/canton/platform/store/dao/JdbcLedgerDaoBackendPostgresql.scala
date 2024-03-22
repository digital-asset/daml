// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.testing.postgresql.PostgresAroundAll
import org.scalatest.AsyncTestSuite

private[dao] trait JdbcLedgerDaoBackendPostgresql
    extends JdbcLedgerDaoBackend
    with PostgresAroundAll {
  this: AsyncTestSuite =>

  override protected val dbType: DbType = DbType.Postgres

  override protected def jdbcUrl: String = postgresDatabase.url
}
