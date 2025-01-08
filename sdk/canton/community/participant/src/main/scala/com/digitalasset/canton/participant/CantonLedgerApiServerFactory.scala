// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.time.*
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.daml.lf.engine.Engine
import org.apache.pekko.actor.ActorSystem

class CantonLedgerApiServerFactory(
    engine: Engine,
    clock: Clock,
    testingTimeService: TestingTimeService,
    allocateIndexerLockIds: DbConfig => Either[String, Option[IndexerLockIds]],
    meteringReportKey: MeteringReportKey,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  def createHaConfig(config: LocalParticipantConfig)(implicit
      traceContext: TraceContext
  ): HaConfig =
    config.storage match {
      case _: DbConfig.H2 =>
        // For H2 the non-unique indexer lock ids are sufficient.
        logger.debug("Not allocating indexer lock IDs on H2 config")
        HaConfig()

      case dbConfig: DbConfig =>
        allocateIndexerLockIds(dbConfig).fold(
          err => throw new IllegalStateException(s"Failed to allocated lock IDs for indexer: $err"),
          _.fold(HaConfig()) { case IndexerLockIds(mainLockId, workerLockId) =>
            HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
          },
        )

      case _ =>
        logger.debug("Not allocating indexer lock IDs on non-DB config")
        HaConfig()
    }

  def create(
      name: InstanceName,
      participantId: LedgerParticipantId,
      sync: CantonSyncService,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      ledgerApiIndexer: Eval[LedgerApiIndexer],
      config: LocalParticipantConfig,
      parameters: ParticipantNodeParameters,
      metrics: LedgerApiServerMetrics,
      httpApiMetrics: HttpApiMetrics,
      tracerProvider: TracerProvider,
      adminToken: CantonAdminToken,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, String, CantonLedgerApiServerWrapper.LedgerApiServerState] = {

    val ledgerTestingTimeService = (config.testingTime, clock) match {
      case (Some(TestingTimeServiceConfig.MonotonicTime), clock) =>
        Some(new CantonTimeServiceBackend(clock, testingTimeService, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, simClock: SimClock) =>
        Some(new CantonExternalClockBackend(simClock, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, remoteClock: RemoteClock) =>
        Some(new CantonExternalClockBackend(remoteClock, loggerFactory))
      case _ => None
    }

    for {
      ledgerApiServer <- CantonLedgerApiServerWrapper
        .initialize(
          CantonLedgerApiServerWrapper.Config(
            serverConfig = config.ledgerApi,
            jsonApiConfig = config.httpLedgerApi,
            participantId = participantId,
            engine = engine,
            syncService = sync,
            cantonParameterConfig = parameters,
            testingTimeService = ledgerTestingTimeService,
            adminToken = adminToken,
            enableCommandInspection = config.ledgerApi.enableCommandInspection,
            loggerFactory = loggerFactory,
            tracerProvider = tracerProvider,
            metrics = metrics,
            jsonApiMetrics = httpApiMetrics,
            meteringReportKey = meteringReportKey,
            maxDeduplicationDuration = participantNodePersistentState
              .map(_.settingsStore.settings.maxDeduplicationDuration)
              .value
              .getOrElse(
                throw new IllegalArgumentException(s"Unknown maxDeduplicationDuration")
              )
              .toConfig,
            clock = clock,
          ),
          // start ledger API server iff participant replica is active
          startLedgerApiServer = sync.isActive(),
          futureSupervisor = futureSupervisor,
          parameters = parameters,
          ledgerApiStore = participantNodePersistentState.map(_.ledgerApiStore),
          ledgerApiIndexer = ledgerApiIndexer,
        )(executionContext, actorSystem)
        .leftMap { err =>
          // The MigrateOnEmptySchema exception is private, thus match on the expected message
          val errMsg =
            if (
              Option(err.cause).nonEmpty && err.cause.getMessage.contains("migrate-on-empty-schema")
            )
              s"${err.cause.getMessage} Please run `$name.db.migrate` to apply pending migrations"
            else s"$err"
          s"Ledger API server failed to start: $errMsg"
        }
    } yield ledgerApiServer
  }
}
