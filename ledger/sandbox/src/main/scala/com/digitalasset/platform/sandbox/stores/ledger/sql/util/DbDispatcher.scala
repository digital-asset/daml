// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.sql.Connection
import java.util.concurrent.Executors

import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.HikariJdbcConnectionProvider
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

trait DbDispatcher extends AutoCloseable {

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overriden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    * */
  def executeSql[T](sql: Connection => T): Future[T]

  /**
    * Creates a lazy Source, which takes care of:
    * - getting a connection for the stream
    * - run the SQL query using the connection
    * - close the connection when the stream ends
    *
    * @param sql a streaming SQL query
    * @tparam T the type of streamed elements
    * @return a lazy source which will only access the database after it's materialized and run
    */
  def runStreamingSql[T](sql: Connection => Source[T, Future[Done]]): Source[T, NotUsed]

  /** The number of pre-allocated connections for short lived queries */
  def noOfShortLivedConnections: Int
}

private class DbDispatcherImpl(
    jdbcUrl: String,
    val noOfShortLivedConnections: Int,
    noOfStreamingConnections: Int)
    extends DbDispatcher {

  private val logger = LoggerFactory.getLogger(getClass)
  private val connectionProvider =
    HikariJdbcConnectionProvider(jdbcUrl, noOfShortLivedConnections, noOfStreamingConnections)
  private val sqlExecutor = SqlExecutor(noOfShortLivedConnections)

  private val connectionGettingThreadPool = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("JdbcConnectionAccessor")
        .setUncaughtExceptionHandler((thread, t) =>
          logger.error(s"got an uncaught exception on thread: ${thread.getName}", t))
        .build()))

  override def executeSql[T](sql: Connection => T): Future[T] =
    sqlExecutor.runQuery(() => connectionProvider.runSQL(conn => sql(conn)))

  override def runStreamingSql[T](
      sql: Connection => Source[T, Future[Done]]): Source[T, NotUsed] = {
    // Getting a connection can block! Presumably, it only blocks if the connection pool has no free connections.
    // getStreamingConnection calls can therefore not be parallelized, and we use a single thread for all of them.
    Source
      .fromFuture(Future(connectionProvider.getStreamingConnection())(connectionGettingThreadPool))
      .flatMapConcat(conn =>
        sql(conn)
          .mapMaterializedValue { f =>
            f.onComplete(_ => conn.close())(DirectExecutionContext)
            f
        })
  }

  override def close(): Unit = {
    connectionProvider.close()
    sqlExecutor.close()
    connectionGettingThreadPool.shutdown()
  }
}

object DbDispatcher {

  /**
    * A helper class to dispatch blocking SQL queries onto a dedicated thread pool. The number of threads are being kept
    * * in sync with the number of JDBC connections in the pool.
    *
    * @param jdbcUrl                   the jdbc url containing the database name, user name and password
    * @param noOfShortLivedConnections the number of connections to be pre-allocated for regular SQL queries
    * @param noOfStreamingConnections  the max number of connections to be used for long, streaming queries
    */
  def apply(
      jdbcUrl: String,
      noOfShortLivedConnections: Int,
      noOfStreamingConnections: Int): DbDispatcher =
    new DbDispatcherImpl(jdbcUrl, noOfShortLivedConnections, noOfStreamingConnections)
}
