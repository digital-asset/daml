// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.logging.LoggingContext
import com.daml.metrics.DatabaseMetrics
import com.daml.platform.store.dao.DbDispatcher

import java.sql.Connection
import scala.concurrent.Future

object Ops {

  private[localstore] def rollbackOnLeft[E, T](sql: Connection => Either[E, T])(
      connection: Connection
  ): Either[E, T] =
    sql(connection).left.map { error =>
      connection.rollback()
      error
    }

  implicit class DbDispatcherLeftOps(val dbDispatcher: DbDispatcher) extends AnyVal {
    /*
      This method extends DbDispatcher.executeSql to accept a closure which returns Either.
      In case of Left value on that Either - transaction is rolled back.
     */
    def executeSqlEither[E, T](databaseMetrics: DatabaseMetrics)(sql: Connection => Either[E, T])(
        implicit loggingContext: LoggingContext
    ): Future[Either[E, T]] =
      dbDispatcher.executeSql(databaseMetrics)(rollbackOnLeft(sql))
  }
}
