// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection

import com.daml.ledger.on.sql.queries.{ReadQueries, ReadWriteQueries}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.zaxxer.hikari.HikariDataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

sealed abstract class Transactor[Q](
    queries: Connection => Q,
    connectionPool: HikariDataSource,
    metrics: Metrics,
)(implicit ec: ExecutionContext) {

  private val logger = ContextualizedLogger.get(getClass)

  def inTransaction[T](name: String)(body: Q => Try[T])(
      implicit logCtx: LoggingContext,
  ): Future[T] =
    Future {
      withEnrichedLoggingContext(
        "transaction" -> name,
        "connectionPool" -> connectionPool.getPoolName,
      ) { implicit logCtx =>
        // The connection is acquired in the same callback as the
        // one where the query is run. If connection acquisition
        // is turned into its own callback it can cause a deadlock
        // if we have a connection pool with a single available slot
        // and a single-threaded executor: if two connection acquisitions
        // are scheduled one after the other, the second will block
        // the only thread while trying to acquire the connection, while
        // the latter will be unable be dispatched on a thread so that it
        // can be executed and finally release the connection.
        logger.debug(s"Acquiring connection...")
        val connection: Connection =
          Timed.value(
            metrics.daml.ledger.database.transactions.acquireConnection(name),
            connectionPool.getConnection()
          )
        try {
          logger.debug(s"Connection acquired, attempting query...")
          val result = body(queries(connection)).get
          logger.debug(s"Query successful, committing...")
          connection.commit()
          logger.debug(s"Commit successful")
          result
        } catch {
          case NonFatal(e) =>
            logger.debug("Rolling back transaction if connection is open", e)
            if (!connection.isClosed) {
              logger.debug("Connection open, rolling back.")
              connection.rollback()
            }
            throw e
        } finally {
          logger.debug("Checking if connection needs closing...")
          if (!connection.isClosed) {
            logger.debug("Connection not closed yet, closing now.")
            connection.close()
          }
        }
      }
    }

}

object Transactor {

  final class ReadWrite(
      queries: Connection => ReadWriteQueries,
      connectionPool: HikariDataSource,
      metrics: Metrics,
      executionContext: ExecutionContext,
  ) extends Transactor[ReadWriteQueries](queries, connectionPool, metrics)(executionContext)

  final class ReadOnly(
      queries: Connection => ReadQueries,
      connectionPool: HikariDataSource,
      metrics: Metrics,
      executionContext: ExecutionContext,
  ) extends Transactor[ReadQueries](queries, connectionPool, metrics)(executionContext)

}
