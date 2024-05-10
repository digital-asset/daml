// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.lf.engine.Engine
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DbConfig, H2DbConfig}
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.admin.MutablePackageNameMapResolver
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.time.*
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
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
  def create(
      name: InstanceName,
      participantId: LedgerParticipantId,
      sync: CantonSyncService,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      config: LocalParticipantConfig,
      parameters: ParticipantNodeParameters,
      metrics: LedgerApiServerMetrics,
      httpApiMetrics: HttpApiMetrics,
      tracerProvider: TracerProvider,
      adminToken: CantonAdminToken,
      packageNameMapResolver: MutablePackageNameMapResolver,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      traceContext: TraceContext,
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
      // For participants with append-only schema enabled, we allocate lock IDs for the indexer
      indexerLockIds <-
        config.storage match {
          case _: H2DbConfig =>
            // For H2 the non-unique indexer lock ids are sufficient.
            logger.debug("Not allocating indexer lock IDs on H2 config")
            EitherT.rightT[FutureUnlessShutdown, String](None)
          case dbConfig: DbConfig =>
            allocateIndexerLockIds(dbConfig)
              .leftMap { err =>
                s"Failed to allocated lock IDs for indexer: $err"
              }
              .toEitherT[FutureUnlessShutdown]
          case _ =>
            logger.debug("Not allocating indexer lock IDs on non-DB config")
            EitherT.rightT[FutureUnlessShutdown, String](None)
        }

      indexerHaConfig = indexerLockIds.fold(HaConfig()) {
        case IndexerLockIds(mainLockId, workerLockId) =>
          HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
      }

      ledgerApiServer <- CantonLedgerApiServerWrapper
        .initialize(
          CantonLedgerApiServerWrapper.Config(
            serverConfig = config.ledgerApi,
            jsonApiConfig = config.httpLedgerApiExperimental.map(
              _.toConfig(
                config.ledgerApi.tls
                  .map(
                    LedgerApiServerConfig.ledgerApiServerTlsConfigFromCantonServerConfig
                  )
              )
            ),
            indexerConfig = parameters.ledgerApiServerParameters.indexer,
            indexerHaConfig = indexerHaConfig,
            participantId = participantId,
            engine = engine,
            syncService = sync,
            storageConfig = config.storage,
            cantonParameterConfig = parameters,
            testingTimeService = ledgerTestingTimeService,
            adminToken = adminToken,
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
          ),
          // start ledger API server iff participant replica is active
          startLedgerApiServer = sync.isActive(),
          futureSupervisor = futureSupervisor,
          packageNameMapResolver = packageNameMapResolver,
          parameters = parameters,
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
