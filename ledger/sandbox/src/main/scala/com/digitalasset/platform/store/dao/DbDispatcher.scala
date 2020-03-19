// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.codahale.metrics.{MetricRegistry, Timer}
import com.digitalasset.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.resources.ResourceOwner
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class DbDispatcher private (
    val maxConnections: Int,
    connectionProvider: HikariJdbcConnectionProvider,
    sqlExecutor: ExecutorService,
    metrics: MetricRegistry,
)(implicit logCtx: LoggingContext)
    extends ReportsHealth {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val sqlExecution = ExecutionContext.fromExecutorService(sqlExecutor)

  object Metrics {
    val waitAllTimer: Timer = metrics.timer("daml.index.db.all.wait")
    val execAllTimer: Timer = metrics.timer("daml.index.db.all.exec")
  }

  override def currentHealth(): HealthStatus = connectionProvider.currentHealth()

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overridden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    */
  def executeSql[T](description: String, extraLog: => Option[String] = None)(
      sql: Connection => T
  ): Future[T] = {
    lazy val extraLogMemoized = extraLog
    val waitTimer = metrics.timer(s"daml.index.db.$description.wait")
    val execTimer = metrics.timer(s"daml.index.db.$description.exec")
    val startWait = System.nanoTime()
    Future {
      val waitNanos = System.nanoTime() - startWait
      extraLogMemoized.foreach(log =>
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
          extraLogMemoized.foreach(log =>
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
}

object DbDispatcher {
  private val logger = ContextualizedLogger.get(this.getClass)
  def owner(
      jdbcUrl: String,
      maxConnections: Int,
      metrics: MetricRegistry,
  )(implicit logCtx: LoggingContext): ResourceOwner[DbDispatcher] = {
    for {
      connectionProvider <- HikariJdbcConnectionProvider.owner(jdbcUrl, maxConnections, metrics)
      sqlExecutor <- ResourceOwner.forExecutorService(
        () =>
          Executors.newFixedThreadPool(
            maxConnections,
            new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("sql-executor-%d")
              .setUncaughtExceptionHandler((_, e) =>
                logger.error("Got an uncaught exception in SQL executor!", e))
              .build()
        ))
    } yield new DbDispatcher(maxConnections, connectionProvider, sqlExecutor, metrics)
  }
}
