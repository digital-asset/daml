// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.foldable.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{ErrorUtil, retry}
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.duration.*

/** Some of the state of a participant that is not tied to a domain and must survive restarts.
  * Does not cover topology stores (as they are also present for domain nodes)
  * nor the [[RegisteredDomainsStore]] (for initialization reasons)
  */
class ParticipantNodePersistentState private (
    val settingsStore: ParticipantSettingsStore,
    val ledgerApiStore: LedgerApiStore,
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
      inFlightSubmissionStore,
      commandDeduplicationStore,
      pruningStore,
      ledgerApiStore,
    )(logger)
}

object ParticipantNodePersistentState extends HasLoggerName {

  /** Creates a [[ParticipantNodePersistentState]] and initializes the settings store.
    */
  def create(
      storage: Storage,
      storageConfig: StorageConfig,
      exitOnFatalFailures: Boolean,
      maxDeduplicationDurationO: Option[NonNegativeFiniteDuration],
      batching: BatchingConfig,
      releaseProtocolVersion: ReleaseProtocolVersion,
      metrics: ParticipantMetrics,
      ledgerParticipantId: LedgerParticipantId,
      ledgerApiServerConfig: LedgerApiServerConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[ParticipantNodePersistentState] = {
    val settingsStore = ParticipantSettingsStore(
      storage,
      timeouts,
      futureSupervisor,
      exitOnFatalFailures = exitOnFatalFailures,
      loggerFactory,
    )
    val inFlightSubmissionStore = InFlightSubmissionStore(
      storage,
      batching.aggregator,
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
          NoExceptionRetryPolicy,
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
            show"Using the max deduplication duration $storedMaxDeduplication instead of the configured $maxDeduplicationDuration."
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
      ledgerApiStore <- FutureUnlessShutdown.outcomeF(
        LedgerApiStore.initialize(
          storageConfig = storageConfig,
          ledgerParticipantId = ledgerParticipantId,
          legderApiDatabaseConnectionTimeout = ledgerApiServerConfig.databaseConnectionTimeout,
          ledgerApiPostgresDataSourceConfig = ledgerApiServerConfig.postgresDataSource,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
          metrics = metrics.ledgerApiServer,
        )
      )
      _ = flagCloseable.close()
    } yield {
      new ParticipantNodePersistentState(
        settingsStore,
        ledgerApiStore,
        inFlightSubmissionStore,
        commandDeduplicationStore,
        pruningStore,
        timeouts,
        loggerFactory,
      )
    }
  }
}
