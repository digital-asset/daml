// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.File
import java.time.{Clock, Instant}
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.error.{ErrorCodesVersionSwitcher, ValueSwitch}
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, Authorizer}
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
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

import scala.collection.immutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

final class StandaloneApiServer(
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
)(implicit actorSystem: ActorSystem, materializer: Materializer, loggingContext: LoggingContext)
    extends ResourceOwner[ApiServer] {

  private val logger = ContextualizedLogger.get(this.getClass)

  // Name of this participant,
  val participantId: Ref.ParticipantId = config.participantId

  override def acquire()(implicit context: ResourceContext): Resource[ApiServer] = {
    val packageStore = loadDamlPackages()
    preloadPackages(packageStore)

    val valueEnricher = new ValueEnricher(engine)

    val owner = for {
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
          enableAppendOnlySchema = config.enableAppendOnlySchema,
          maxContractStateCacheSize = config.maxContractStateCacheSize,
          maxContractKeyStateCacheSize = config.maxContractKeyStateCacheSize,
          enableMutableContractStateCache = config.enableMutableContractStateCache,
          maxTransactionsInMemoryFanOutBufferSize = config.maxTransactionsInMemoryFanOutBufferSize,
          enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
        )
        .map(index => new SpannedIndexService(new TimedIndexService(index, metrics)))
      authorizer = new Authorizer(
        Clock.systemUTC.instant _,
        ledgerId,
        participantId,
        new ErrorCodesVersionSwitcher(config.enableSelfServiceErrorCodes),
      )
      healthChecksWithIndexService = healthChecks + ("index" -> indexService)
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
          executionContext,
          new ValueSwitch(config.enableSelfServiceErrorCodes),
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

    owner.acquire()
  }

  private def preloadPackages(packageContainer: InMemoryPackageStore): Unit = {
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

  private def loadDamlPackages(): InMemoryPackageStore = {
    // TODO is it sensible to have all the initial packages to be known since the epoch?
    config.archiveFiles
      .foldLeft[Either[(String, File), InMemoryPackageStore]](Right(InMemoryPackageStore.empty)) {
        case (storeE, f) =>
          storeE.flatMap(_.withDarFile(Instant.now(), None, f).left.map(_ -> f))
      }
      .fold({ case (err, file) => sys.error(s"Could not load package $file: $err") }, identity)
  }

  private def writePortFile(port: Port): Try[Unit] = {
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
}
