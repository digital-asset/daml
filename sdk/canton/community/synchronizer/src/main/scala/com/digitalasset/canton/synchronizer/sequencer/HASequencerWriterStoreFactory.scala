// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  ProcessingTimeout,
  QueryCostMonitoringConfig,
}
import com.digitalasset.canton.crypto.PseudoRandom
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.resource.DbLockedConnectionPool.NoActiveConnectionAvailable
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.SequencerHighAvailabilityConfig
import com.digitalasset.canton.synchronizer.sequencer.WriterStartupError.AllInstanceIndexesInUse
import com.digitalasset.canton.synchronizer.sequencer.store.{
  HASequencerWriterStore,
  SequencerStore,
  SequencerWriterStore,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Pause}
import com.digitalasset.canton.util.{BytesUnit, EitherTUtil}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.{ExecutionContext, Future}

/** [[com.digitalasset.canton.synchronizer.sequencer.store.SequencerWriterStore]] factory using
  * [[resource.DbStorageMulti]] to ensure each writer has a unique instance index and that all
  * sensitive writes are performed while holding an exclusive lock for that instance index.
  */
class HASequencerWriterStoreFactory(
    protocolVersion: ProtocolVersion,
    haConfig: SequencerHighAvailabilityConfig,
    logQueryCost: Option[QueryCostMonitoringConfig],
    scheduler: Option[ScheduledExecutorService],
    sequencerMember: Member,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    sequencerMetrics: SequencerMetrics,
    protected val timeouts: ProcessingTimeout,
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerWriterStoreFactory
    with FlagCloseable
    with HasCloseContext
    with NamedLogging {

  private val defaultMinRetry = NonNegativeFiniteDuration.tryOfMillis(200)

  override def create(storage: Storage, generalStore: SequencerStore)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore] =
    storage match {
      // locking support is required to support the highly available sequencer
      case dbStorage: DbStorage if isLockingSupported(dbStorage) =>
        eventuallyCreateExclusiveWriter(dbStorage)
      case _other =>
        // We've got here as the configuration specifies a high-availability config but we're
        // running with a storage instance that does not support locks so cannot use our dynamic node assignment.
        // We're intentionally throwing here as the sequencer writer will never run with this configuration and
        // shouldn't be retried.
        EitherT.leftT(
          WriterStartupError.DatabaseNotSupported(
            "Must configure Postgres storage to use a highly available sequencer"
          )
        )
    }

  private def isLockingSupported(dbStorage: DbStorage): Boolean =
    // we're going to use this to throw an exception about this storage profile not supporting locking for our highly
    // available sequencer so are fine throwing away this Left(error-message)
    DbLock.isSupported(dbStorage.profile).isRight

  override protected def onClosed(): Unit = ()

  /** is expected that callers have ensured this db-storage implementation supports locking. */
  private def eventuallyCreateExclusiveWriter(storage: DbStorage)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore] = {
    val logger = TracedLogger(getClass, loggerFactory)

    EitherT {
      Pause(logger, this, Int.MaxValue, defaultMinRetry.toScala, "createExclusiveWriter")
        .unlessShutdown(
          createExclusiveWriter(storage, loggerFactory).value,
          AllExceptionRetryPolicy,
        )
    }
  }

  private def createExclusiveWriter(
      storage: DbStorage,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, WriterStartupError, SequencerWriterStore] =
    for {
      instanceIndex <- pickAvailableIndex(storage, loggerFactory)
      _ = logger.debug(s"Picked index $instanceIndex")
      writePoolSize =
        storage.dbConfig.numWriteConnectionsCanton(
          forParticipant = false,
          withWriteConnectionPool = true,
          withMainConnection = true,
        )
      readPoolSize =
        storage.dbConfig.numReadConnectionsCanton(
          forParticipant = false,
          withWriteConnectionPool = true,
          withMainConnection = false,
        )

      unusedSessionContext = CloseContext(FlagCloseable(logger, timeouts))
      multiStorage <- DbStorageMulti
        .create(
          storage.dbConfig,
          haConfig.connectionPool,
          readPoolSize,
          writePoolSize,
          DbLockCounters.SEQUENCER_WRITERS_MAIN(instanceIndex),
          DbLockCounters.SEQUENCER_WRITERS_POOL(instanceIndex),
          () => FutureUnlessShutdown.unit,
          () => FutureUnlessShutdown.unit,
          storage.metrics,
          logQueryCost,
          None,
          scheduler,
          timeouts,
          exitOnFatalFailures = exitOnFatalFailures,
          futureSupervisor,
          loggerFactory.append("writerId", PseudoRandom.randomAlphaNumericString(4)),
          () => unusedSessionContext,
        )
        .leftMap(WriterStartupError.FailedToCreateExclusiveStorage.apply)
        .mapK(FutureUnlessShutdown.liftK)
      _ <- failIfNotActive(multiStorage)
        .leftMap(WriterStartupError.FailedToCreateExclusiveStorage.apply)
        .leftWiden[WriterStartupError]
        .mapK(FutureUnlessShutdown.outcomeK)
      _ = logger.debug(s"Got lock for $instanceIndex")
    } yield new HASequencerWriterStore(
      instanceIndex,
      multiStorage,
      protocolVersion,
      sequencerMember,
      cachingConfigs,
      batchingConfig,
      sequencerMetrics,
      timeouts,
      loggerFactory,
      this.closeContext,
    )

  private def pickAvailableIndex(storage: DbStorage, loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, AllInstanceIndexesInUse, Int] = {
    // TODO(#15837): Support HA for unified sequencer
    val store = SequencerStore(
      storage,
      protocolVersion,
      BytesUnit.zero, // Events cache is not currently supported in HA mode
      SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
      timeouts,
      loggerFactory,
      sequencerMember,
      blockSequencerMode = false,
      cachingConfigs,
      batchingConfig,
      sequencerMetrics,
    )

    for {
      onlineInstances <- EitherT.right(store.fetchOnlineInstances)
      availableInstances = (0 until haConfig.totalNodeCount.unwrap).filterNot(
        onlineInstances.contains
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        availableInstances.nonEmpty,
        (),
        AllInstanceIndexesInUse("There are no available sequencer instance indexes available"),
      )
    } yield {
      // we opt to pick a random index from the available sequencers to reduce contention when starting
      // many at once.
      // e.g. if they all attempted to use 0 at once only one would win and the rest would retry.
      // (although this instance picking should still be safe+functional if that index selection
      //  strategy was used - it would just take longer due to numerous retries.)
      availableInstances(Math.floor(Math.random() * availableInstances.size).toInt)
    }
  }

  private def failIfNotActive(
      multiStorage: DbStorageMulti
  )(implicit executionContext: ExecutionContext): EitherT[Future, String, Unit] =
    if (multiStorage.isActive) EitherTUtil.unit[String]
    else {
      multiStorage.close()
      EitherT.leftT("Lock could not be acquired when creating the storage instance")
    }

  override def expectedOfflineException(error: Throwable): Boolean = error match {
    // thrown by [[DbStorageMulti]] when a write requiring the exclusive writer lock is attempted
    case _: PassiveInstanceException | _: NoActiveConnectionAvailable => true
    case _ => false
  }
}
