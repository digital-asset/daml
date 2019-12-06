// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.codahale.metrics.{MetricRegistry, Timer}
import com.digitalasset.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.HikariJdbcConnectionProvider
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class DbDispatcher(
    val maxConnections: Int,
    connectionProvider: HikariJdbcConnectionProvider,
    sqlExecutor: ExecutorService,
    logger: Logger,
    metrics: MetricRegistry,
) extends AutoCloseable
    with ReportsHealth {
  private val sqlExecution = ExecutionContext.fromExecutorService(sqlExecutor)

  object Metrics {
    val waitAllTimer: Timer = metrics.timer("sql_all_wait")
    val execAllTimer: Timer = metrics.timer("sql_all_exec")
  }

  override def currentHealth(): HealthStatus = connectionProvider.currentHealth()

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overridden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    */
  def executeSql[T](description: String, extraLog: Option[String] = None)(
      sql: Connection => T
  ): Future[T] = {
    val waitTimer = metrics.timer(s"sql_${description}_wait")
    val execTimer = metrics.timer(s"sql_${description}_exec")
    val startWait = System.nanoTime()
    Future {
      val waitNanos = System.nanoTime() - startWait
      extraLog.foreach(log =>
        logger.trace(s"$description: $log wait ${(waitNanos / 1E6).toLong} ms"))
      waitTimer.update(waitNanos, TimeUnit.NANOSECONDS)
      Metrics.waitAllTimer.update(waitNanos, TimeUnit.NANOSECONDS)
      val startExec = System.nanoTime()
      try {
        // Actual execution
        val result = connectionProvider.runSQL(sql)
        result
      } catch {
        case NonFatal(e) =>
          logger.error(
            s"$description: Got an exception while executing a SQL query. Rolled back the transaction.",
            e)
          throw e
        // fatal errors don't make it for some reason to the setUncaughtExceptionHandler
        case t: Throwable =>
          logger.error(s"$description: got a fatal error!", t)
          throw t
      } finally {
        // decouple metrics updating from sql execution above
        try {
          val execNanos = System.nanoTime() - startExec
          extraLog.foreach(log =>
            logger.trace(s"$description: $log exec ${(execNanos / 1E6).toLong} ms"))
          execTimer.update(execNanos, TimeUnit.NANOSECONDS)
          Metrics.execAllTimer.update(execNanos, TimeUnit.NANOSECONDS)
        } catch {
          case t: Throwable =>
            logger
              .error(s"$description: Got an exception while updating timer metrics. Ignoring.", t)
        }
      }
    }(sqlExecution)
  }

  override def close(): Unit = {
    sqlExecutor.shutdown()
    connectionProvider.close()
  }
}

object DbDispatcher {
  def start(
      jdbcUrl: String,
      maxConnections: Int,
      loggerFactory: NamedLoggerFactory,
      metrics: MetricRegistry,
  ): DbDispatcher = {
    val logger = loggerFactory.getLogger(classOf[DbDispatcher])

    val connectionProvider = HikariJdbcConnectionProvider.start(jdbcUrl, maxConnections, metrics)

    lazy val sqlExecutor =
      Executors.newFixedThreadPool(
        maxConnections,
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("sql-executor-%d")
          .setUncaughtExceptionHandler((_, e) =>
            logger.error("Got an uncaught exception in SQL executor!", e))
          .build()
      )

    new DbDispatcher(maxConnections, connectionProvider, sqlExecutor, logger, metrics)
  }
}
