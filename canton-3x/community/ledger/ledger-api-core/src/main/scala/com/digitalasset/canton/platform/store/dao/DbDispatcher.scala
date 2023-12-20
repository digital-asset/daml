// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.error.ContextualizedErrorLogger
import com.daml.executors.InstrumentedExecutors
import com.daml.executors.executors.{
  NamedExecutor,
  QueueAwareExecutionContextExecutorService,
  QueueAwareExecutor,
}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.entries.LoggingEntry
import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.ResourceOwnerOps
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.sql.Connection
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[platform] trait DbDispatcher {
  val executor: QueueAwareExecutor with NamedExecutor
  def executeSql[T](databaseMetrics: DatabaseMetrics)(sql: Connection => T)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[T]

}

private[dao] final class DbDispatcherImpl private[dao] (
    connectionProvider: JdbcConnectionProvider,
    val executor: QueueAwareExecutionContextExecutorService,
    overallWaitTimer: Timer,
    overallExecutionTimer: Timer,
    val loggerFactory: NamedLoggerFactory,
) extends DbDispatcher
    with ReportsHealth
    with NamedLogging {

  private val executionContext = ExecutionContext.fromExecutor(
    executor,
    throwable =>
      logger.error("ExecutionContext has failed with an exception", throwable)(TraceContext.empty),
  )

  override def currentHealth(): HealthStatus =
    connectionProvider.currentHealth()

  /** Runs an SQL statement in a dedicated Executor. The whole block will be run in a single database transaction.
    *
    * The isolation level by default is the one defined in the JDBC driver, it can be however overridden per query on
    * the Connection. See further details at: https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
    */
  def executeSql[T](databaseMetrics: DatabaseMetrics)(
      sql: Connection => T
  )(implicit loggingContext: LoggingContextWithTrace): Future[T] =
    withEnrichedLoggingContext(("metric" -> databaseMetrics.name): LoggingEntry) {
      implicit loggingContext: LoggingContextWithTrace =>
        val startWait = System.nanoTime()
        Future {
          val waitNanos = System.nanoTime() - startWait
          logger.trace(s"Waited ${(waitNanos / 1e6).toLong} ms to acquire connection.")
          databaseMetrics.waitTimer.update(waitNanos, TimeUnit.NANOSECONDS)
          overallWaitTimer.update(waitNanos, TimeUnit.NANOSECONDS)
          val startExec = System.nanoTime()
          try {
            connectionProvider.runSQL(databaseMetrics)(sql)
          } catch {
            case throwable: Throwable => handleError(throwable)
          } finally {
            updateMetrics(databaseMetrics, startExec)
          }
        }(executionContext)
    }

  private def updateMetrics(databaseMetrics: DatabaseMetrics, startExec: Long)(implicit
      traceContext: TraceContext
  ): Unit =
    try {
      val execNanos = System.nanoTime() - startExec
      logger.trace(s"Executed query in ${(execNanos / 1e6).toLong} ms")
      databaseMetrics.executionTimer.update(execNanos, TimeUnit.NANOSECONDS)
      overallExecutionTimer.update(execNanos, TimeUnit.NANOSECONDS)
    } catch {
      case NonFatal(e) =>
        logger.info("Got an exception while updating timer metrics. Ignoring.", e)
    }

  private def handleError(
      throwable: Throwable
  )(implicit loggingContext: LoggingContextWithTrace): Nothing = {
    implicit val errorLoggingContext: ContextualizedErrorLogger =
      ErrorLoggingContext(logger, loggingContext)

    throwable match {
      case NonFatal(e) => throw DatabaseSelfServiceError(e)
      // fatal errors don't make it for some reason to the setUncaughtExceptionHandler
      case t: Throwable =>
        logger.error("Fatal error!", t)
        throw t
    }
  }
}

object DbDispatcher {

  def owner(
      dataSource: DataSource,
      serverRole: ServerRole,
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
      metrics: Metrics,
      loggerFactory: NamedLoggerFactory,
  ): ResourceOwner[DbDispatcher with ReportsHealth] = {
    val logger = loggerFactory.getTracedLogger(getClass)
    def log(s: String): Unit =
      logger.debug(s"[${serverRole.threadPoolSuffix}] $s")(TraceContext.empty)
    for {
      hikariDataSource <- HikariDataSourceOwner(
        dataSource = dataSource,
        serverRole = serverRole,
        minimumIdle = connectionPoolSize,
        maxPoolSize = connectionPoolSize,
        connectionTimeout = connectionTimeout,
        metrics = Some(metrics.registry),
      ).afterReleased(log("HikariDataSource released"))
      connectionProvider <- DataSourceConnectionProvider
        .owner(
          hikariDataSource,
          serverRole.threadPoolSuffix,
          loggerFactory,
        )
        .afterReleased(log("DataSourceConnectionProvider released"))
      threadPoolName = MetricName(
        metrics.daml.index.db.threadpool.connection,
        serverRole.threadPoolSuffix,
      )
      executor <- ResourceOwner
        .forExecutorService(
          () =>
            InstrumentedExecutors.newFixedThreadPoolWithFactory(
              threadPoolName,
              connectionPoolSize,
              new ThreadFactoryBuilder()
                .setNameFormat(s"$threadPoolName-%d")
                .setUncaughtExceptionHandler((_, e) =>
                  loggerFactory
                    .getTracedLogger(getClass)
                    .error("Uncaught exception in the SQL executor.", e)(TraceContext.empty)
                )
                .build(),
              metrics.executorServiceMetrics,
            ),
          gracefulAwaitTerminationMillis =
            5000, // waiting 5s for ongoing SQL operations to finish and then forcing them with Thread.interrupt...
          forcefulAwaitTerminationMillis = 5000, // ...and then waiting 5s more
        )
        .afterReleased(log("ExecutorService released"))
    } yield new DbDispatcherImpl(
      connectionProvider = connectionProvider,
      executor = executor,
      overallWaitTimer = metrics.daml.index.db.waitAll,
      overallExecutionTimer = metrics.daml.index.db.execAll,
      loggerFactory = loggerFactory,
    )
  }
}
