// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.platform.store.DbType
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.Suite

private[dao] trait JdbcLedgerDaoBackendPostgresql
    extends JdbcLedgerDaoBackend
    with PostgresAroundAll { this: Suite =>

  override protected val dbType: DbType = DbType.Postgres
  override protected def jdbcUrl: String = postgresDatabase.url

}
