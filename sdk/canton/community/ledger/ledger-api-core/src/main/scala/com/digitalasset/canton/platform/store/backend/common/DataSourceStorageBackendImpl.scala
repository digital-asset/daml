// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.get
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

private[backend] object DataSourceStorageBackendImpl {

  def exe(statement: String): Connection => Unit = { implicit connection =>
    SQL"#$statement".execute().discard
  }

  def checkDatabaseAvailable(connection: Connection): Unit =
    assert(SQL"SELECT 1".as(get[Int](1).single)(connection) == 1)
}
