// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.lf.engine.Engine
import com.daml.tracing.DefaultOpenTelemetry
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.http.JsonApiConfig
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.platform.apiserver.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.tracing.{NoTracing, TracerProvider}
import org.apache.pekko.actor.ActorSystem

import scala.util.{Failure, Success}

/** Wrapper of ledger API server to manage start, stop, and erasing of state.
  */
object CantonLedgerApiServerWrapper extends NoTracing {
  final case class IndexerLockIds(mainLockId: Int, workerLockId: Int)

  /** Config for ledger API server and indexer
    *
    * @param serverConfig          ledger API server configuration
    * @param jsonApiConfig         JSON API configuration
    * @param indexerConfig         indexer configuration
    * @param indexerHaConfig       configuration for indexer HA
    * @param participantId         unique participant id used e.g. for a unique ledger API server index db name
    * @param engine                daml engine shared with Canton for performance reasons
    * @param syncService           canton sync service implementing both read and write services
    * @param storageConfig         canton storage config so that indexer can share the participant db
    * @param cantonParameterConfig configurations meant to be overridden primarily in tests (applying to all participants)
    * @param testingTimeService    an optional service during testing for advancing time, participant-specific
    * @param adminToken            canton admin token for ledger api auth
    * @param enableCommandInspection     whether canton should support inspection service or not
    * @param loggerFactory         canton logger factory
    * @param tracerProvider        tracer provider for open telemetry grpc injection
    * @param metrics               upstream metrics module
    */
  final case class Config(
      serverConfig: LedgerApiServerConfig,
      jsonApiConfig: Option[JsonApiConfig],
      indexerConfig: IndexerConfig,
      indexerHaConfig: HaConfig,
      participantId: LedgerParticipantId,
      engine: Engine,
      syncService: CantonSyncService,
      storageConfig: StorageConfig,
      cantonParameterConfig: ParticipantNodeParameters,
      testingTimeService: Option[TimeServiceBackend],
      adminToken: CantonAdminToken,
      enableCommandInspection: Boolean,
      override val loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
      metrics: LedgerApiServerMetrics,
      jsonApiMetrics: HttpApiMetrics,
      meteringReportKey: MeteringReportKey,
      maxDeduplicationDuration: NonNegativeFiniteDuration,
  ) extends NamedLogging {
    override def logger: TracedLogger = super.logger

  }

  /** Initialize a ledger API server asynchronously
    *
    * @param config ledger API server configuration
    * @param startLedgerApiServer whether to start the ledger API server or not
    *              (i.e. when participant node is initialized in passive mode)
    * @return ledger API server state wrapper EitherT-future
    */
  def initialize(
      config: Config,
      parameters: ParticipantNodeParameters,
      startLedgerApiServer: Boolean,
      futureSupervisor: FutureSupervisor,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, LedgerApiServerError, LedgerApiServerState] = {

    val ledgerApiStorageE = LedgerApiStorage.fromStorageConfig(
      config.storageConfig,
      config.participantId,
    )

    EitherT
      .fromEither[FutureUnlessShutdown](ledgerApiStorageE)
      .flatMap { ledgerApiStorage =>
        val connectionPoolConfig = DbSupport.ConnectionPoolConfig(
          connectionPoolSize = config.storageConfig.numConnectionsLedgerApiServer.unwrap,
          connectionTimeout = config.serverConfig.databaseConnectionTimeout.underlying,
        )

        val dbConfig = DbSupport.DbConfig(
          jdbcUrl = ledgerApiStorage.jdbcUrl,
          connectionPool = connectionPoolConfig,
          postgres = config.serverConfig.postgresDataSource,
        )

        val participantDataSourceConfig =
          DbSupport.ParticipantDataSourceConfig(ledgerApiStorage.jdbcUrl)

        implicit val tracer = config.tracerProvider.tracer

        val startableStoppableLedgerApiServer =
          new StartableStoppableLedgerApiServer(
            config = config,
            participantDataSourceConfig = participantDataSourceConfig,
            dbConfig = dbConfig,
            telemetry = new DefaultOpenTelemetry(config.tracerProvider.openTelemetry),
            futureSupervisor = futureSupervisor,
            parameters = parameters,
            commandProgressTracker = config.syncService.commandProgressTracker,
          )
        val startFUS = for {
          _ <-
            if (startLedgerApiServer) startableStoppableLedgerApiServer.start()
            else FutureUnlessShutdown.unit
        } yield ()

        EitherT(startFUS.transformWith {
          case Success(_) =>
            FutureUnlessShutdown.pure(
              Either.right(
                LedgerApiServerState(
                  ledgerApiStorage,
                  startableStoppableLedgerApiServer,
                  config.logger,
                  config.cantonParameterConfig.processingTimeouts,
                )
              )
            )
          case Failure(e) => FutureUnlessShutdown.pure(Left(FailedToStartLedgerApiServer(e)))
        })
      }
  }

  final case class LedgerApiServerState(
      ledgerApiStorage: LedgerApiStorage,
      startableStoppableLedgerApi: StartableStoppableLedgerApiServer,
      override protected val logger: TracedLogger,
      protected override val timeouts: ProcessingTimeout,
  ) extends FlagCloseable {

    override protected def onClosed(): Unit =
      Lifecycle.close(startableStoppableLedgerApi, ledgerApiStorage)(logger)

    override def toString: String = getClass.getSimpleName
  }

  sealed trait LedgerApiServerError extends Product with Serializable with PrettyPrinting {
    protected def errorMessage: String = ""
    def cause: Throwable
    def asRuntimeException(additionalMessage: String = ""): RuntimeException =
      new RuntimeException(
        if (additionalMessage.isEmpty) errorMessage else s"$additionalMessage $errorMessage",
        cause,
      )
  }

  sealed trait LedgerApiServerErrorWithoutCause extends LedgerApiServerError {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    override def cause: Throwable = null
  }

  final case class FailedToStartLedgerApiServer(cause: Throwable) extends LedgerApiServerError {
    override def pretty: Pretty[FailedToStartLedgerApiServer] = prettyOfClass(unnamedParam(_.cause))
  }

  final case class FailedToStopLedgerApiServer(
      override protected val errorMessage: String,
      cause: Throwable,
  ) extends LedgerApiServerError {
    override def pretty: Pretty[FailedToStopLedgerApiServer] =
      prettyOfClass(param("error", _.errorMessage.unquoted), param("cause", _.cause))
  }

  final case class FailedToConfigureLedgerApiStorage(override protected val errorMessage: String)
      extends LedgerApiServerErrorWithoutCause {
    override def pretty: Pretty[FailedToConfigureLedgerApiStorage] =
      prettyOfClass(unnamedParam(_.errorMessage.unquoted))
  }
}
