// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import cats.syntax.foldable.*
import cats.syntax.parallel.*
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
      uniqueContractKeysO: Option[Boolean],
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
      uniqueContractKeysO: Option[Boolean],
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
      uniqueContractKeysO,
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
    *
    * @param uniqueContractKeysO If [[scala.Some$]], try to set the unique contract key mode accordingly,
    *                            but if the participant has previously been connected to a domain,
    *                            the domain's parameter for unique contract keys takes precedence.
    *                            If [[scala.None$]], skip storing and checking the UCK setting.
    */
  def create(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      storage: Storage,
      clock: Clock,
      maxDeduplicationDurationO: Option[NonNegativeFiniteDuration],
      uniqueContractKeysO: Option[Boolean],
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

    def setUniqueContractKeysSetting(uniqueContractKeys: Boolean): FutureUnlessShutdown[Unit] = {
      def checkStoredSetting(stored: Boolean): FutureUnlessShutdown[Unit] = {
        if (uniqueContractKeys != stored) {
          logger.warn(
            show"Using unique-contract-keys=$stored instead of the configured $uniqueContractKeys.\nThis indicates that the participant was previously started with unique-contract-keys=$stored and then the configuration was changed."
          )
        }
        FutureUnlessShutdown.unit
      }

      if (storage.isActive) {
        for {
          // Figure out whether the participant has previously been connected to a UCK or a non-UCK domain.
          ucksByDomain <- syncDomainPersistentStates.getAll.toSeq.parTraverseFilter {
            case (domainId, state) =>
              FutureUnlessShutdown.outcomeF(
                state.parameterStore.lastParameters.map(
                  _.map(param => domainId -> param.uniqueContractKeys)
                )
              )
          }
          (nonUckDomains, uckDomains) = ucksByDomain.partitionMap { case (domainId, isUck) =>
            Either.cond(isUck, domainId, domainId)
          }
          toBePersisted =
            if (uckDomains.nonEmpty) {
              if (!uniqueContractKeys) {
                logger.warn(
                  show"Ignoring participant config of not enforcing unique contract keys as the participant has been previously connected to domains with unique contract keys: $uckDomains"
                )
              }
              true
            } else if (nonUckDomains.nonEmpty) {
              if (uniqueContractKeys) {
                logger.warn(
                  show"Ignoring participant config of enforcing unique contract keys as the participant has been previously connected to domains without unique contract keys: $nonUckDomains"
                )
              }
              false
            } else uniqueContractKeys
          _ <- settingsStore.settings.uniqueContractKeys match {
            case None => settingsStore.insertUniqueContractKeysMode(toBePersisted)
            case Some(persisted) => checkStoredSetting(persisted)
          }
        } yield ()
      } else {
        waitForSettingsStoreUpdate(_.uniqueContractKeys, "unique contract key config").flatMap(
          checkStoredSetting
        )
      }
    }

    for {
      _ <- settingsStore.refreshCache()
      _ <- uniqueContractKeysO.traverse_(setUniqueContractKeysSetting)
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
