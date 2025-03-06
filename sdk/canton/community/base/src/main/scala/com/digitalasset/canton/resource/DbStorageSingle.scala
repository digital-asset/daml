// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, QueryCostMonitoringConfig}
import com.digitalasset.canton.data.CantonTimestamp
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
import com.digitalasset.canton.resource.DbStorage.{DbStorageCreationException, Profile}
import com.digitalasset.canton.time.{Clock, PeriodicAction}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{LoggerUtil, ResourceUtil}
import slick.jdbc.JdbcBackend.Database

import java.sql.SQLException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.DurationConverters.JavaDurationOps

/** DB Storage implementation that assumes a single process accessing the underlying database. */
final class DbStorageSingle private (
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
  private val timeWhenFailureStartedRef = new AtomicReference[Option[CantonTimestamp]](None)

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

  override protected[canton] def runRead[A](
      action: ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    run("reading", operationName, maxRetries)(
      FutureUnlessShutdown.outcomeF(db.run(action))
    )

  override protected[canton] def runWrite[A](
      action: All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] =
    run("writing", operationName, maxRetries)(
      FutureUnlessShutdown.outcomeF(db.run(action))
    )

  override def onClosed(): Unit = {
    periodicConnectionCheck.close()
    db.close()
  }

  override def isActive: Boolean = isActiveRef.get()

  private def checkConnectivity(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(
      Future(blocking(try {
        // FIXME(i11240): if db is backed by a connection pool, this can fail even if the db is healthy, because the pool is busy executing long-running queries
        val connection =
          // this will timeout and throw a SQLException if can't establish a connection
          db.source.createConnection()
        val valid = ResourceUtil.withResource(connection)(
          _.isValid(dbConfig.parameters.connectionTimeout.duration.toSeconds.toInt)
        )
        if (valid) {
          timeWhenFailureStartedRef.set(None)
          resolveUnhealthy()
        }
        valid
      } catch {
        case e: SQLException =>
          val failedToFatalDelay = dbConfig.parameters.failedToFatalDelay.duration
          val now = clock.now

          val failureDurationExceededDelay = timeWhenFailureStartedRef.getAndUpdate {
            case previous @ Some(_) => previous
            case None => Some(now)
          } match {
            case None => false
            case Some(timeWhenFailureStarted) =>
              val failureDuration = (now - timeWhenFailureStarted).toScala
              logger.debug(
                s"Storage has been failing since $timeWhenFailureStarted (${LoggerUtil.roundDurationForHumans(failureDuration)} ago)"
              )
              failureDuration > failedToFatalDelay
          }

          if (failureDurationExceededDelay)
            fatalOccurred(
              s"Storage failed for more than ${LoggerUtil.roundDurationForHumans(failedToFatalDelay)}"
            )
          else failureOccurred(DatabaseConnectionLost(e.getMessage))

          false
      })).map { active =>
        val old = isActiveRef.getAndSet(active)
        val changed = old != active
        if (changed)
          logger.info(s"Changed db storage instance to ${if (active) "active" else "passive"}.")
      }
    )
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

    val profile = DbStorage.profile(config)

    val readOnlyCheckConfigO = profile match {
      case _: Profile.Postgres =>
        // Enable periodic read-only connection check on hikari for postgres
        DbConfig
          .toConfig(
            Map(
              // Run a query that checks if transaction_read_only is on and fail the test query with an exception.
              "connectionTestQuery" ->
                """
                |DO $$DECLARE read_only text;
                |BEGIN
                | select current_setting('transaction_read_only') into read_only;
                | if read_only = 'on' then
                |  raise 'connection is read-only';
                | end if;
                |END$$;
                """.stripMargin,
              // Run the connection test query every 30 seconds
              "keepaliveTime" -> "30000",
            )
          )
          .some

      case _ => None
    }

    for {
      db <- DbStorage.createDatabase(
        config,
        numCombined,
        Some(metrics.general),
        logQueryCost,
        scheduler,
        retryConfig = retryConfig,
        additionalConfigDefaults = readOnlyCheckConfigO,
      )(loggerFactory)
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
