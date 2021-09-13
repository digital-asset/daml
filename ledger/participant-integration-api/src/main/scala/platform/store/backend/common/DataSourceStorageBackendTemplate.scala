// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.get
import com.daml.platform.store.backend.DataSourceStorageBackend
import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

private[backend] trait DataSourceStorageBackendTemplate extends DataSourceStorageBackend {

  protected def exe(statement: String): Connection => Unit = { connection =>
    val stmnt = connection.createStatement()
    try {
      stmnt.execute(statement)
      ()
    } finally {
      stmnt.close()
    }
  }

  override def checkDatabaseAvailable(connection: Connection): Unit =
    assert(SQL"SELECT 1".as(get[Int](1).single)(connection) == 1)
}
