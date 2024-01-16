// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateLookup
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import com.digitalasset.canton.util.{ErrorUtil, retry}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** Some of the state of a participant that is not tied to a domain and must survive restarts.
  * Does not cover topology stores (as they are also present for domain nodes)
  * nor the [[RegisteredDomainsStore]] (for initialization reasons)
  */
class ParticipantNodePersistentState private (
    val settingsStore: ParticipantSettingsStore,
    val participantEventLog: ParticipantEventLog,
    val multiDomainEventLog: MultiDomainEventLog,
    val inFlightSubmissionStore: InFlightSubmissionStore,
    val commandDeduplicationStore: CommandDeduplicationStore,
    val pruningStore: ParticipantPruningStore,
    override val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {
  override def onClosed(): Unit =
    Lifecycle.close(
      settingsStore,
      participantEventLog,
      multiDomainEventLog,
      inFlightSubmissionStore,
      commandDeduplicationStore,
      pruningStore,
    )(logger)
}

trait ParticipantNodePersistentStateFactory {
  def create(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      storage: Storage,
      clock: Clock,
      maxDeduplicationDurationO: Option[NonNegativeFiniteDuration],
      batching: BatchingConfig,
      parameters: ParticipantStoreConfig,
      releaseProtocolVersion: ReleaseProtocolVersion,
      metrics: ParticipantMetrics,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Eval[ParticipantNodePersistentState]]
}

object ParticipantNodePersistentStateFactory extends ParticipantNodePersistentStateFactory {
  override def create(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      storage: Storage,
      clock: Clock,
      maxDeduplicationDurationO: Option[NonNegativeFiniteDuration],
      batching: BatchingConfig,
      parameters: ParticipantStoreConfig,
      releaseProtocolVersion: ReleaseProtocolVersion,
      metrics: ParticipantMetrics,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Eval[ParticipantNodePersistentState]] = ParticipantNodePersistentState
    .create(
      syncDomainPersistentStates,
      storage,
      clock,
      maxDeduplicationDurationO,
      batching,
      parameters,
      releaseProtocolVersion,
      metrics,
      indexedStringStore,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    .map(Eval.now)
}

object ParticipantNodePersistentState extends HasLoggerName {

  /** Creates a [[ParticipantNodePersistentState]] and initializes the settings store.
    */
  def create(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      storage: Storage,
      clock: Clock,
      maxDeduplicationDurationO: Option[NonNegativeFiniteDuration],
      batching: BatchingConfig,
      parameters: ParticipantStoreConfig,
      releaseProtocolVersion: ReleaseProtocolVersion,
      metrics: ParticipantMetrics,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[ParticipantNodePersistentState] = {
    val settingsStore = ParticipantSettingsStore(storage, timeouts, futureSupervisor, loggerFactory)
    val participantEventLog =
      ParticipantEventLog(
        storage,
        indexedStringStore,
        releaseProtocolVersion,
        timeouts,
        loggerFactory,
      )
    val inFlightSubmissionStore = InFlightSubmissionStore(
      storage,
      batching.maxItemsInSqlClause,
      parameters.dbBatchAggregationConfig,
      releaseProtocolVersion,
      timeouts,
      loggerFactory,
    )
    val commandDeduplicationStore = CommandDeduplicationStore(
      storage,
      timeouts,
      releaseProtocolVersion,
      loggerFactory,
    )
    val pruningStore = ParticipantPruningStore(storage, timeouts, loggerFactory)

    implicit val loggingContext: NamedLoggingContext =
      NamedLoggingContext(loggerFactory, traceContext)
    val logger = loggingContext.tracedLogger
    val flagCloseable = FlagCloseable(logger, timeouts)
    implicit val closeContext: CloseContext = CloseContext(flagCloseable)

    def waitForSettingsStoreUpdate[A](
        lens: ParticipantSettingsStore.Settings => Option[A],
        settingName: String,
    ): FutureUnlessShutdown[A] =
      retry
        .Pause(
          logger,
          flagCloseable,
          timeouts.activeInit.retries(50.millis),
          50.millis,
          functionFullName,
        )
        .unlessShutdown(
          settingsStore.refreshCache().map(_ => lens(settingsStore.settings).toRight(())),
          NoExnRetryable,
        )
        .map(_.getOrElse {
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Passive replica failed to read $settingName, needs to be written by active replica"
            )
          )
        })

    def checkOrSetMaxDedupDuration(
        maxDeduplicationDuration: NonNegativeFiniteDuration
    ): FutureUnlessShutdown[Unit] = {

      def checkStoredMaxDedupDuration(
          storedMaxDeduplication: NonNegativeFiniteDuration
      ): FutureUnlessShutdown[Unit] = {
        if (maxDeduplicationDuration != storedMaxDeduplication) {
          logger.warn(
            show"Using the max deduplication duration ${storedMaxDeduplication} instead of the configured $maxDeduplicationDuration."
          )
        }
        FutureUnlessShutdown.unit
      }

      if (storage.isActive) {
        settingsStore.settings.maxDeduplicationDuration match {
          case None => settingsStore.insertMaxDeduplicationDuration(maxDeduplicationDuration)
          case Some(storedMaxDeduplication) =>
            checkStoredMaxDedupDuration(storedMaxDeduplication)
        }
      } else {
        // On the passive replica wait for the max deduplication duration to be written by the active replica
        waitForSettingsStoreUpdate(_.maxDeduplicationDuration, "max deduplication duration")
          .flatMap(checkStoredMaxDedupDuration)
      }
    }

    for {
      _ <- settingsStore.refreshCache()
      _ <- maxDeduplicationDurationO.traverse_(checkOrSetMaxDedupDuration)
      multiDomainEventLog <- FutureUnlessShutdown.outcomeF(
        MultiDomainEventLog.create(
          syncDomainPersistentStates,
          participantEventLog,
          storage,
          clock,
          metrics,
          indexedStringStore,
          timeouts,
          futureSupervisor,
          loggerFactory,
        )
      )
      _ = flagCloseable.close()
    } yield {
      new ParticipantNodePersistentState(
        settingsStore,
        participantEventLog,
        multiDomainEventLog,
        inFlightSubmissionStore,
        commandDeduplicationStore,
        pruningStore,
        timeouts,
        loggerFactory,
      )
    }
  }
}
