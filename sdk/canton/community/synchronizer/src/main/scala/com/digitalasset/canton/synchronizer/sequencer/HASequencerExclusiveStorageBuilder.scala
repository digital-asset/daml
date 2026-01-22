// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{ProcessingTimeout, QueryCostMonitoringConfig}
import com.digitalasset.canton.crypto.PseudoRandom
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  HasCloseContext,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.synchronizer.sequencer.HASequencerExclusiveStorageBuilder.*
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.DatabaseSequencerExclusiveStorageConfig
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext

/** Provides exclusive storage to HA sequencers for configuration stored in the database and
  * pruning.
  */
private[sequencer] class HASequencerExclusiveStorageBuilder(
    exclusiveStorageConfig: DatabaseSequencerExclusiveStorageConfig,
    logQueryCost: Option[QueryCostMonitoringConfig],
    schedulerForMetricsCollection: ScheduledExecutorService,
    protected val timeouts: ProcessingTimeout,
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with HasCloseContext
    with NamedLogging {

  // if we're configured for high availability, create "exclusive storage" to choose
  // one of the sequencers to perform database-backed configuration changes and pruning.
  // By default lower default maxConnections for read and write pools.
  def create(storage: Storage): Either[CreateError, ExclusiveStorage] =
    TraceContext.withNewTraceContext("create_ha_sequencer") { implicit traceContext =>
      for {
        dbStorageAndConfig <- storage match {
          // locking support is required to support the highly available sequencer
          case dbStorage: DbStorage if isLockingSupported(dbStorage) =>
            Right((dbStorage, dbStorage.dbConfig))
          case _other =>
            Left(
              ProvidedDbStorageDoesNotSupportHA(
                "Must configure Postgres storage to use a highly available sequencer"
              )
            )
        }

        (dbStorage, dbConfig) = dbStorageAndConfig

        failoverNotifier = new HASequencerExclusiveStorageNotifier(loggerFactory)

        unusedSessionContext = CloseContext(FlagCloseable(logger, timeouts))
        multiStorage <- DbStorageMulti
          .create(
            dbConfig = dbConfig,
            writeConnectionPoolConfig = exclusiveStorageConfig.connectionPool,
            writePoolSize = dividedMaxConnections,
            readPoolSize = dividedMaxConnections,
            mainLockCounter = DbLockCounters.SEQUENCER_PRUNING_SCHEDULER_WRITE,
            poolLockCounter = DbLockCounters.SEQUENCER_PRUNING_SCHEDULER_WRITERS,
            onActive = () => failoverNotifier.onActive(),
            onPassive = () => failoverNotifier.onPassive(),
            metrics = dbStorage.metrics,
            logQueryCost = logQueryCost,
            customClock = None,
            scheduler = Some(schedulerForMetricsCollection),
            timeouts = timeouts,
            exitOnFatalFailures = exitOnFatalFailures,
            futureSupervisor = futureSupervisor,
            loggerFactory =
              loggerFactory.append("exclusiveWriterId", PseudoRandom.randomAlphaNumericString(4)),
            getSessionContext = () => unusedSessionContext,
          )
          .value match {
          case UnlessShutdown.Outcome(either) => either.leftMap(FailedToCreateDbStorageMulti.apply)
          case UnlessShutdown.AbortedDueToShutdown => Left(ShutdownInProgress)
        }
      } yield ExclusiveStorage(multiStorage, failoverNotifier)
    }

  // Divide max connections among read and write pools.
  private def dividedMaxConnections: PositiveInt =
    PositiveInt.tryCreate((exclusiveStorageConfig.maxConnections.value / 2) max 1)

  override protected def onClosed(): Unit = ()

  private def isLockingSupported(dbStorage: DbStorage): Boolean =
    // we're going to use this to error about this storage profile not supporting locking for our highly
    // available sequencer so are fine throwing away this Left(error-message)
    DbLock.isSupported(dbStorage.profile).isRight

}

private[sequencer] object HASequencerExclusiveStorageBuilder {

  /** Exclusive storage
    * @param storage
    *   actual db storage multi
    * @param failoverNotifier
    *   failover notifier allows delayed registering for notifications once the components using
    *   exclusive storage are built
    */
  final case class ExclusiveStorage(
      storage: DbStorageMulti,
      failoverNotifier: HASequencerExclusiveStorageNotifier,
  )

  trait CreateError {
    def message: String
  }

  final case class DbStorageConfigSpecifiesNonHADatabase(message: String) extends CreateError
  final case class ProvidedDbStorageDoesNotSupportHA(message: String) extends CreateError
  final case class FailedToCreateDbStorageMulti(message: String) extends CreateError
  object ShutdownInProgress extends CreateError {
    override lazy val message =
      "Shutdown in progress so not creating exclusive HA sequencer storage"
  }
}
