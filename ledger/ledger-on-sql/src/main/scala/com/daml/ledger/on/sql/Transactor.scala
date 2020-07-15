// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection

import com.daml.ledger.on.sql.queries.{ReadQueries, ReadWriteQueries}
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

sealed abstract class Transactor[Q](
    queries: Connection => Q,
    connectionPool: DataSource,
    metrics: Metrics,
)(implicit ec: ExecutionContext) {

  def inTransaction[T](name: String)(body: Q => Try[T])(
      implicit logCtx: LoggingContext): Future[T] =
    Future {
      // Connection is acquired in the same future to ensure that
      // two connection acquisition are not accidentally scheduled
      // one after the other, causing a deadlock on a connection
      // pool with a single available slot (the first connection
      // is acquired but cannot be scheduled, the second connection
      // acquisition is scheduled but cannot be performed until
      // the first connection is used and made available again)
      val connection = Timed.value(
        metrics.daml.ledger.database.transactions.acquireConnection(name),
        connectionPool.getConnection()
      )
      try {
        val result = body(queries(connection)).get
        connection.commit()
        result
      } catch {
        case NonFatal(e) =>
          connection.rollback()
          throw e
      } finally {
        connection.close()
      }
    }

}

object Transactor {

  final class ReadWrite(
      queries: Connection => ReadWriteQueries,
      connectionPool: DataSource,
      metrics: Metrics,
      executionContext: ExecutionContext,
  ) extends Transactor[ReadWriteQueries](queries, connectionPool, metrics)(executionContext)

  final class ReadOnly(
      queries: Connection => ReadQueries,
      connectionPool: DataSource,
      metrics: Metrics,
      executionContext: ExecutionContext,
  ) extends Transactor[ReadQueries](queries, connectionPool, metrics)(executionContext)

}
