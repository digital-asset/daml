// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.File
import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, Authorizer}
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId, SeedService, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.Engine
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.{
  CommandConfiguration,
  LedgerConfiguration,
  PartyConfiguration,
  ServerRole
}
import com.daml.ports.{PortFiles}
import com.daml.platform.index.JdbcIndex
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.ports.Port
import io.grpc.{BindableService, ServerInterceptor}

import scala.collection.immutable

// Main entry point to start an index server that also hosts the ledger API.
// See v2.ReferenceServer on how it is used.
final class StandaloneApiServer(
    ledgerId: LedgerId,
    config: ApiServerConfig,
    commandConfig: CommandConfiguration,
    partyConfig: PartyConfiguration,
    ledgerConfig: LedgerConfiguration,
    optWriteService: Option[WriteService],
    authService: AuthService,
    healthChecks: HealthChecks,
    metrics: Metrics,
    timeServiceBackend: Option[TimeServiceBackend] = None,
    otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
    otherInterceptors: List[ServerInterceptor] = List.empty,
    engine: Engine,
    lfValueTranslationCache: LfValueTranslation.Cache,
)(implicit actorSystem: ActorSystem, materializer: Materializer, loggingContext: LoggingContext)
    extends ResourceOwner[ApiServer] {

  private val logger = ContextualizedLogger.get(this.getClass)

  // Name of this participant,
  val participantId: ParticipantId = config.participantId

  override def acquire()(implicit context: ResourceContext): Resource[ApiServer] = {
    val packageStore = loadDamlPackages()
    preloadPackages(packageStore)

    val owner = for {
      indexService <- JdbcIndex
        .owner(
          ServerRole.ApiServer,
          domain.LedgerId(ledgerId),
          participantId,
          config.jdbcUrl,
          config.eventsPageSize,
          metrics,
          lfValueTranslationCache,
        )
        .map(index => new SpannedIndexService(new TimedIndexService(index, metrics)))
      authorizer = new Authorizer(Clock.systemUTC.instant _, ledgerId, participantId)
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
            TimeProviderType.Static),
        ledgerConfiguration = ledgerConfig,
        commandConfig = commandConfig,
        partyConfig = partyConfig,
        optTimeServiceBackend = timeServiceBackend,
        metrics = metrics,
        healthChecks = healthChecksWithIndexService,
        seedService = SeedService(config.seeding),
        managementServiceTimeout = config.managementServiceTimeout,
      )(materializer, executionSequencerFactory, loggingContext)
        .map(_.withServices(otherServices))
      apiServer <- new LedgerApiServer(
        apiServicesOwner,
        config.port,
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig,
        AuthorizationInterceptor(authService, executionContext) :: otherInterceptors,
        metrics
      )
    } yield {
      writePortFile(apiServer.port)
      logger.info(
        s"Initialized API server version ${BuildInfo.Version} with ledger-id = $ledgerId, port = ${apiServer.port}, dar file = ${config.archiveFiles}")
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
          packageContainer.getLfPackageSync, { _ =>
            sys.error("Unexpected request of contract key")
          }
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

  private def writePortFile(port: Port): Unit =
    config.portFile.foreach { path =>
      PortFiles.write(path, port)
    }
}
