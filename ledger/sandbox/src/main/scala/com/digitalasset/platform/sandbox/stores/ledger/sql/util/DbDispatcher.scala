// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.sql.Connection
import java.util.concurrent.Executors

import com.codahale.metrics.MetricRegistry
import com.digitalasset.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.HikariJdbcConnectionProvider
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, Future}

/**
  * A helper class to dispatch blocking SQL queries onto a dedicated thread pool.
  * The number of threads are being kept in sync with the number of JDBC connections in the pool.
  *
  * @param jdbcUrl                    the JDBC url containing the database name, user name and password
  * @param noOfShortLivedConnections the number of connections to be pre-allocated for regular SQL queries
  */
final class DbDispatcher(
    jdbcUrl: String,
    val noOfShortLivedConnections: Int,
    loggerFactory: NamedLoggerFactory,
    metrics: MetricRegistry,
) extends AutoCloseable
    with ReportsHealth {

  private val logger = loggerFactory.getLogger(getClass)
  private val connectionProvider =
    new HikariJdbcConnectionProvider(jdbcUrl, noOfShortLivedConnections, metrics)
  private val sqlExecutor = new SqlExecutor(noOfShortLivedConnections, loggerFactory, metrics)

  private val connectionGettingThreadPool = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("JdbcConnectionAccessor")
        .setUncaughtExceptionHandler((thread, t) =>
          logger.error(s"got an uncaught exception on thread: ${thread.getName}", t))
        .build()))

  override def currentHealth(): HealthStatus = sqlExecutor.currentHealth()

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overridden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    */
  def executeSql[T](description: String, extraLog: Option[String] = None)(
      sql: Connection => T): Future[T] =
    sqlExecutor.runQuery(description, extraLog)(connectionProvider.runSQL(sql))

  override def close(): Unit = {
    connectionProvider.close()
    sqlExecutor.close()
    connectionGettingThreadPool.shutdown()
  }
}
