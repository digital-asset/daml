// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.sql.Connection
import java.util.concurrent.{Executor, Executors, TimeUnit}

import com.codahale.metrics.Timer
import com.daml.ledger.api.health.{HealthStatus, ReportsHealth}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.configuration.ServerRole
import com.daml.resources.ResourceOwner
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

final class DbDispatcher private (
    val maxConnections: Int,
    connectionProvider: HikariJdbcConnectionProvider,
    executor: Executor,
    overallWaitTimer: Timer,
    overallExecutionTimer: Timer,
)(implicit logCtx: LoggingContext)
    extends ReportsHealth {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val executionContext = ExecutionContext.fromExecutor(executor)

  override def currentHealth(): HealthStatus = connectionProvider.currentHealth()

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overridden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    */
  def executeSql[T](databaseMetrics: DatabaseMetrics, extraLog: => Option[String] = None)(
      sql: Connection => T
  ): Future[T] = {
    lazy val extraLogMemoized = extraLog
    val startWait = System.nanoTime()
    Future {
      val waitNanos = System.nanoTime() - startWait
      extraLogMemoized.foreach(log =>
        logger.trace(s"${databaseMetrics.name}: $log wait ${(waitNanos / 1E6).toLong} ms"))
      databaseMetrics.waitTimer.update(waitNanos, TimeUnit.NANOSECONDS)
      overallWaitTimer.update(waitNanos, TimeUnit.NANOSECONDS)
      val startExec = System.nanoTime()
      try {
        // Actual execution
        val result = connectionProvider.runSQL(sql)
        result
      } catch {
        case NonFatal(e) =>
          logger.error(
            s"${databaseMetrics.name}: Got an exception while executing a SQL query. Rolled back the transaction.",
            e)
          throw e
        // fatal errors don't make it for some reason to the setUncaughtExceptionHandler
        case t: Throwable =>
          logger.error(s"${databaseMetrics.name}: got a fatal error!", t)
          throw t
      } finally {
        // decouple metrics updating from sql execution above
        try {
          val execNanos = System.nanoTime() - startExec
          extraLogMemoized.foreach(log =>
            logger.trace(s"${databaseMetrics.name}: $log exec ${(execNanos / 1E6).toLong} ms"))
          databaseMetrics.executionTimer.update(execNanos, TimeUnit.NANOSECONDS)
          overallExecutionTimer.update(execNanos, TimeUnit.NANOSECONDS)
        } catch {
          case NonFatal(e) =>
            logger
              .error(
                s"${databaseMetrics.name}: Got an exception while updating timer metrics. Ignoring.",
                e)
        }
      }
    }(executionContext)
  }
}

object DbDispatcher {
  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      maxConnections: Int,
      metrics: Metrics,
  )(implicit logCtx: LoggingContext): ResourceOwner[DbDispatcher] =
    for {
      connectionProvider <- HikariJdbcConnectionProvider.owner(
        serverRole,
        jdbcUrl,
        maxConnections,
        metrics.registry)
      executor <- ResourceOwner.forExecutorService(
        () =>
          Executors.newFixedThreadPool(
            maxConnections,
            new ThreadFactoryBuilder()
              .setNameFormat(s"daml.index.db.connection.${serverRole.threadPoolSuffix}-%d")
              .setUncaughtExceptionHandler((_, e) =>
                logger.error("Uncaught exception in the SQL executor.", e))
              .build()
        ))
    } yield
      new DbDispatcher(
        maxConnections = maxConnections,
        connectionProvider = connectionProvider,
        executor = executor,
        overallWaitTimer = metrics.daml.index.db.waitAll,
        overallExecutionTimer = metrics.daml.index.db.execAll,
      )
}
