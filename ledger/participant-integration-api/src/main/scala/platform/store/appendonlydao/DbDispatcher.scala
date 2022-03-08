// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.health.{HealthStatus, ReportsHealth}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform.configuration.ServerRole
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.prometheus.client.Summary

import java.sql.Connection
import java.util.concurrent.{Executor, Executors}
import javax.sql.DataSource
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[platform] final class DbDispatcher private (
    connectionProvider: JdbcConnectionProvider,
    executor: Executor,
    overallWaitTimer: Summary,
    overallExecutionTimer: Summary,
)(implicit loggingContext: LoggingContext)
    extends SqlExecutor
    with ReportsHealth {

  private val logger = ContextualizedLogger.get(this.getClass)
  private val executionContext = ExecutionContext.fromExecutor(
    executor,
    throwable => logger.error("ExecutionContext has failed with an exception", throwable),
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
  )(implicit loggingContext: LoggingContext): Future[T] =
    withEnrichedLoggingContext("metric" -> databaseMetrics.name) { implicit loggingContext =>
      val waitTimer: Summary.Timer = databaseMetrics.waitTimer.startTimer()
      Future {
        val waitSeconds: Double = waitTimer.observeDuration()
        logger.trace(s"Waited ${(waitSeconds * 1e3).toLong} ms to acquire connection")
        overallWaitTimer.observe(waitSeconds)
        val executionTimer: Summary.Timer = databaseMetrics.executionTimer.startTimer()
        try {
          connectionProvider.runSQL(databaseMetrics)(sql)
        } catch {
          case throwable: Throwable => handleError(throwable)
        } finally {
          updateExecutionMetrics(executionTimer)
        }
      }(executionContext)
    }

  private def updateExecutionMetrics(execTimer: Summary.Timer)(implicit
      loggingContext: LoggingContext
  ): Unit =
    try {
      val execSeconds: Double = execTimer.observeDuration()
      logger.trace(s"Executed query in ${(execSeconds * 1e3).toLong} ms")
      overallExecutionTimer.observe(execSeconds)
    } catch {
      case NonFatal(e) =>
        logger.info("Got an exception while updating timer metrics. Ignoring.", e)
    }

  private def handleError(
      throwable: Throwable
  )(implicit loggingContext: LoggingContext): Nothing = {
    implicit val contextualizedLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)

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
  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(
      dataSource: DataSource,
      serverRole: ServerRole,
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): ResourceOwner[DbDispatcher] =
    for {
      hikariDataSource <- HikariDataSourceOwner(
        dataSource,
        serverRole,
        connectionPoolSize,
        connectionPoolSize,
        connectionTimeout,
//        Some(metrics.registry),
      )
      connectionProvider <- DataSourceConnectionProvider.owner(hikariDataSource)
      threadPoolName = s"daml.index.db.threadpool.connection.${serverRole.threadPoolSuffix}"
      executor <- ResourceOwner.forExecutorService(() =>
        Executors.newFixedThreadPool(
          connectionPoolSize,
          new ThreadFactoryBuilder()
            .setNameFormat(s"$threadPoolName-%d")
            .setUncaughtExceptionHandler((_, e) =>
              logger.error("Uncaught exception in the SQL executor.", e)
            )
            .build(),
        )
      // TODO Prometheus metrics: implement a replacement
//        new InstrumentedExecutorService(
//          Executors.newFixedThreadPool(
//            connectionPoolSize,
//            new ThreadFactoryBuilder()
//              .setNameFormat(s"$threadPoolName-%d")
//              .setUncaughtExceptionHandler((_, e) =>
//                logger.error("Uncaught exception in the SQL executor.", e)
//              )
//              .build(),
//          ),
////          metrics.registry,
//          threadPoolName,
//        )
      )
    } yield new DbDispatcher(
      connectionProvider = connectionProvider,
      executor = executor,
      overallWaitTimer = metrics.daml.index.db.waitAll,
      overallExecutionTimer = metrics.daml.index.db.execAll,
    )
}
