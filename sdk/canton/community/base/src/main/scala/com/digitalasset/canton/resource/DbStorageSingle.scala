// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, QueryCostMonitoringConfig}
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.resource.DatabaseStorageError.DatabaseConnectionLost.DatabaseConnectionLost
import com.digitalasset.canton.resource.DbStorage.DbAction.{All, ReadTransactional}
import com.digitalasset.canton.resource.DbStorage.{DbAction, DbStorageCreationException}
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.{Clock, PeriodicAction}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import slick.jdbc.JdbcBackend.Database

import java.sql.SQLException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, blocking}

/** DB Storage implementation that assumes a single process accessing the underlying database. */
class DbStorageSingle private (
    override val profile: DbStorage.Profile,
    override val dbConfig: DbConfig,
    db: Database,
    clock: Clock,
    override protected val logOperations: Boolean,
    override val metrics: DbStorageMetrics,
    override protected val timeouts: ProcessingTimeout,
    override val threadsAvailableForWriting: PositiveInt,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends DbStorage
    with FlagCloseable
    with NamedLogging {

  private val isActiveRef = new AtomicReference[Boolean](true)

  override lazy val initialHealthState: ComponentHealthState =
    if (isActiveRef.get()) ComponentHealthState.Ok()
    else ComponentHealthState.failed("instance is passive")

  private val periodicConnectionCheck = new PeriodicAction(
    clock,
    // using the same interval for connection timeout as for periodic check
    dbConfig.parameters.connectionTimeout.toInternal,
    loggerFactory,
    timeouts,
    "db-connection-check",
  )(tc => checkConnectivity(tc))

  // TODO(#18629) Rename this method
  /** this will be renamed once all instances of [[runRead]] has been deprecated */
  override protected[canton] def runReadUnlessShutdown[A](
      action: ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    runUnlessShutdown("reading", operationName, maxRetries)(
      FutureUnlessShutdown.outcomeF(db.run(action))
    )

  // TODO(#18629) Rename this method
  /** this will be renamed once all instances of [[runWrite]] has been deprecated */
  override protected[canton] def runWriteUnlessShutdown[A](
      action: All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    runUnlessShutdown("writing", operationName, maxRetries)(
      FutureUnlessShutdown.outcomeF(db.run(action))
    )

  // TODO(#18629) Remove this method
  /** this will be removed, use [[runReadUnlessShutdown]] instead */
  override protected[canton] def runRead[A](
      action: DbAction.ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    run("reading", operationName, maxRetries)(db.run(action))

  // TODO(#18629) Remove this method
  /** this will be removed, use [[runWriteUnlessShutdown]] instead */
  override protected[canton] def runWrite[A](
      action: DbAction.All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    run("writing", operationName, maxRetries)(db.run(action))

  override def onClosed(): Unit = {
    periodicConnectionCheck.close()
    db.close()
  }

  override def isActive: Boolean = isActiveRef.get()

  private def checkConnectivity(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    Future(blocking(try {
      // FIXME(i11240): if db is backed by a connection pool, this can fail even if the db is healthy, because the pool is busy executing long-running queries
      val connection =
        // this will timeout and throw a SQLException if can't establish a connection
        db.source.createConnection()
      val valid = ResourceUtil.withResource(connection)(
        _.isValid(dbConfig.parameters.connectionTimeout.duration.toSeconds.toInt)
      )
      if (valid) resolveUnhealthy()
      valid
    } catch {
      case e: SQLException =>
        failureOccurred(DatabaseConnectionLost(e.getMessage))
        false
    })).map { active =>
      val old = isActiveRef.getAndSet(active)
      val changed = old != active
      if (changed)
        logger.info(s"Changed db storage instance to ${if (active) "active" else "passive"}.")
    }
  }

}

object DbStorageSingle {
  def tryCreate(
      config: DbConfig,
      clock: Clock,
      scheduler: Option[ScheduledExecutorService],
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      retryConfig: DbStorage.RetryConfig = DbStorage.RetryConfig.failFast,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): DbStorageSingle =
    create(
      config,
      connectionPoolForParticipant,
      logQueryCost,
      clock,
      scheduler,
      metrics,
      timeouts,
      loggerFactory,
      retryConfig,
    )
      .valueOr(err => throw new DbStorageCreationException(err))
      .onShutdown(throw new DbStorageCreationException("Shutdown during creation"))

  def create(
      config: DbConfig,
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      clock: Clock,
      scheduler: Option[ScheduledExecutorService],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      retryConfig: DbStorage.RetryConfig = DbStorage.RetryConfig.failFast,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, String, DbStorageSingle] = {
    val numCombined = config.numCombinedConnectionsCanton(
      connectionPoolForParticipant,
      withWriteConnectionPool = false,
      withMainConnection = false,
    )
    val logger = loggerFactory.getTracedLogger(getClass)
    logger.info(s"Creating storage, num-combined: $numCombined")
    for {
      db <- DbStorage.createDatabase(
        config,
        numCombined,
        Some(metrics.general),
        logQueryCost,
        scheduler,
        retryConfig = retryConfig,
      )(loggerFactory)
      profile = DbStorage.profile(config)
      storage = new DbStorageSingle(
        profile,
        config,
        db,
        clock,
        logQueryCost.exists(_.logOperations),
        metrics,
        timeouts,
        numCombined,
        loggerFactory,
      )
    } yield storage
  }

}
