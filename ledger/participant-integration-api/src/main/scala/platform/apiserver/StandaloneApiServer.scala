// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.error.ErrorCodesVersionSwitcher
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, Authorizer}
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.{
  CommandConfiguration,
  PartyConfiguration,
  ServerRole,
  SubmissionConfiguration,
}
import com.daml.platform.index.JdbcIndex
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.LfValueTranslationCache
import com.daml.ports.{Port, PortFiles}
import io.grpc.{BindableService, ServerInterceptor}
import scalaz.{-\/, \/-}
import java.io.File
import java.time.Clock

import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.telemetry.TelemetryContext

import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

object StandaloneApiServer {
  private val logger = ContextualizedLogger.get(this.getClass)

  def apply(
      ledgerId: LedgerId,
      config: ApiServerConfig,
      commandConfig: CommandConfiguration,
      partyConfig: PartyConfiguration,
      submissionConfig: SubmissionConfiguration,
      optWriteService: Option[state.WriteService],
      authService: AuthService,
      healthChecks: HealthChecks,
      metrics: Metrics,
      timeServiceBackend: Option[TimeServiceBackend] = None,
      otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
      otherInterceptors: List[ServerInterceptor] = List.empty,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      checkOverloaded: TelemetryContext => Option[state.SubmissionResult] =
        _ => None, // Used for Canton rate-limiting
  )(implicit
      actorSystem: ActorSystem,
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[ApiServer] = {
    for {
      indexService <- this.indexService(
        ledgerId = ledgerId,
        config = config,
        metrics = metrics,
        engine = engine,
        servicesExecutionContext = servicesExecutionContext,
        lfValueTranslationCache = lfValueTranslationCache,
      )
      apiServer <- from(
        indexService = indexService,
        ledgerId = ledgerId,
        config = config,
        commandConfig = commandConfig,
        partyConfig = partyConfig,
        submissionConfig = submissionConfig,
        optWriteService = optWriteService,
        authService = authService,
        healthChecks = healthChecks,
        metrics = metrics,
        timeServiceBackend = timeServiceBackend,
        otherServices = otherServices,
        otherInterceptors = otherInterceptors,
        engine = engine,
        servicesExecutionContext = servicesExecutionContext,
        checkOverloaded = checkOverloaded,
      )
    } yield apiServer
  }

  def indexService(
      ledgerId: LedgerId,
      config: ApiServerConfig,
      metrics: Metrics,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexService] = {
    val participantId: Ref.ParticipantId = config.participantId
    val valueEnricher = new ValueEnricher(engine)

    def preloadPackages(packageContainer: InMemoryPackageStore): Unit = {
      for {
        (pkgId, _) <- packageContainer.listLfPackagesSync()
        pkg <- packageContainer.getLfPackageSync(pkgId)
      } {
        engine
          .preloadPackage(pkgId, pkg)
          .consume(
            { _ =>
              sys.error("Unexpected request of contract")
            },
            packageContainer.getLfPackageSync,
            { _ =>
              sys.error("Unexpected request of contract key")
            },
          )
        ()
      }
    }

    def loadDamlPackages(): InMemoryPackageStore = {
      // TODO is it sensible to have all the initial packages to be known since the epoch?
      config.archiveFiles
        .foldLeft[Either[(String, File), InMemoryPackageStore]](Right(InMemoryPackageStore.empty)) {
          case (storeE, f) =>
            storeE.flatMap(_.withDarFile(Timestamp.now(), None, f).left.map(_ -> f))
        }
        .fold({ case (err, file) => sys.error(s"Could not load package $file: $err") }, identity)
    }

    for {
      _ <- ResourceOwner.forValue(() => {
        val packageStore = loadDamlPackages()
        preloadPackages(packageStore)
      })
      indexService <- JdbcIndex
        .owner(
          serverRole = ServerRole.ApiServer,
          ledgerId = domain.LedgerId(ledgerId),
          participantId = participantId,
          jdbcUrl = config.jdbcUrl,
          databaseConnectionPoolSize = config.databaseConnectionPoolSize,
          databaseConnectionTimeout = config.databaseConnectionTimeout,
          eventsPageSize = config.eventsPageSize,
          eventsProcessingParallelism = config.eventsProcessingParallelism,
          servicesExecutionContext = servicesExecutionContext,
          metrics = metrics,
          lfValueTranslationCache = lfValueTranslationCache,
          enricher = valueEnricher,
          maxContractStateCacheSize = config.maxContractStateCacheSize,
          maxContractKeyStateCacheSize = config.maxContractKeyStateCacheSize,
          enableMutableContractStateCache = config.enableMutableContractStateCache,
          maxTransactionsInMemoryFanOutBufferSize = config.maxTransactionsInMemoryFanOutBufferSize,
          enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
          enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
        )
        .map(index => new SpannedIndexService(new TimedIndexService(index, metrics)))
    } yield indexService
  }

  def from(
      indexService: IndexService,
      ledgerId: LedgerId,
      config: ApiServerConfig,
      commandConfig: CommandConfiguration,
      partyConfig: PartyConfiguration,
      submissionConfig: SubmissionConfiguration,
      optWriteService: Option[state.WriteService],
      authService: AuthService,
      healthChecks: HealthChecks,
      metrics: Metrics,
      timeServiceBackend: Option[TimeServiceBackend] = None,
      otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
      otherInterceptors: List[ServerInterceptor] = List.empty,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
      checkOverloaded: TelemetryContext => Option[state.SubmissionResult] =
        _ => None, // Used for Canton rate-limiting
  )(implicit
      actorSystem: ActorSystem,
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[ApiServer] = {
    val participantId: Ref.ParticipantId = config.participantId

    def writePortFile(port: Port): Try[Unit] = {
      config.portFile match {
        case Some(path) =>
          PortFiles.write(path, port) match {
            case -\/(err) => Failure(new RuntimeException(err.toString))
            case \/-(()) => Success(())
          }
        case None =>
          Success(())
      }
    }

    val errorCodesVersionSwitcher = new ErrorCodesVersionSwitcher(
      config.enableSelfServiceErrorCodes
    )
    val authorizer = new Authorizer(
      Clock.systemUTC.instant _,
      ledgerId,
      participantId,
      errorCodesVersionSwitcher,
    )
    val healthChecksWithIndexService = healthChecks + ("index" -> indexService)

    for {
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner()
      apiServicesOwner = new ApiServices.Owner(
        participantId = participantId,
        optWriteService = optWriteService,
        indexService = indexService,
        authorizer = authorizer,
        engine = engine,
        timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        timeProviderType =
          timeServiceBackend.fold[TimeProviderType](TimeProviderType.WallClock)(_ =>
            TimeProviderType.Static
          ),
        configurationLoadTimeout = config.configurationLoadTimeout,
        initialLedgerConfiguration = config.initialLedgerConfiguration,
        commandConfig = commandConfig,
        partyConfig = partyConfig,
        submissionConfig = submissionConfig,
        optTimeServiceBackend = timeServiceBackend,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        healthChecks = healthChecksWithIndexService,
        seedService = SeedService(config.seeding),
        managementServiceTimeout = config.managementServiceTimeout,
        enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
        checkOverloaded = checkOverloaded,
      )(materializer, executionSequencerFactory, loggingContext)
        .map(_.withServices(otherServices))
      apiServer <- new LedgerApiServer(
        apiServicesOwner,
        config.port,
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig,
        AuthorizationInterceptor(
          authService,
          materializer.executionContext,
          errorCodesVersionSwitcher,
        ) :: otherInterceptors,
        servicesExecutionContext,
        metrics,
      )
      _ <- ResourceOwner.forTry(() => writePortFile(apiServer.port))
    } yield {
      logger.info(
        s"Initialized API server version ${BuildInfo.Version} with ledger-id = $ledgerId, port = ${apiServer.port}, dar file = ${config.archiveFiles}"
      )
      apiServer
    }
  }

}
