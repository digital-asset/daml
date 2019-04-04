// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.sql.Connection

import com.digitalasset.platform.sandbox.stores.ledger.sql.migration.HikariJdbcConnectionProvider

import scala.concurrent.Future

/** A helper class to dispatch blocking SQL queries onto a dedicated thread pool. The number of threads are being kept
  * in sync with the number of JDBC connections in the pool. */
class DbDispatcher(jdbcUrl: String, jdbcUser: String, noOfConnections: Int) {
  private val connectionProvider = HikariJdbcConnectionProvider(jdbcUrl, jdbcUser, noOfConnections)

  private val sqlExecutor = SqlExecutor(noOfConnections)

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overriden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    * */
  def executeSql[T](sql: Connection => T): Future[T] =
    sqlExecutor.runQuery(() => connectionProvider.runSQL(conn => sql(conn)))

}

object DbDispatcher {
  def apply(jdbcUrl: String, jdbcUser: String, noOfConnections: Int): DbDispatcher =
    new DbDispatcher(jdbcUrl, jdbcUser, noOfConnections)
}
