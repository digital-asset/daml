// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.File
import java.nio.file.Files
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, Authorizer}
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService, SeedService, WriteService}
import com.daml.lf.engine.Engine
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.{
  CommandConfiguration,
  LedgerConfiguration,
  PartyConfiguration,
  ServerRole,
}
import com.daml.platform.index.JdbcIndex
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.daml.resources.{Resource, ResourceOwner}
import io.grpc.{BindableService, ServerInterceptor}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.ExecutionContext

// Main entry point to start an index server that also hosts the ledger API.
// See v2.ReferenceServer on how it is used.
final class StandaloneApiServer(
    config: ApiServerConfig,
    commandConfig: CommandConfiguration,
    partyConfig: PartyConfiguration,
    ledgerConfig: LedgerConfiguration,
    readService: ReadService,
    writeService: WriteService,
    authService: AuthService,
    transformIndexService: IndexService => IndexService = identity,
    metrics: Metrics,
    timeServiceBackend: Option[TimeServiceBackend] = None,
    otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
    otherInterceptors: List[ServerInterceptor] = List.empty,
    engine: Engine
)(implicit actorSystem: ActorSystem, materializer: Materializer, logCtx: LoggingContext)
    extends ResourceOwner[ApiServer] {

  private val logger = ContextualizedLogger.get(this.getClass)

  // Name of this participant,
  val participantId: ParticipantId = config.participantId

  override def acquire()(implicit executionContext: ExecutionContext): Resource[ApiServer] = {
    val packageStore = loadDamlPackages()
    preloadPackages(packageStore)

    val owner = for {
      initialConditions <- ResourceOwner.forFuture(() =>
        readService.getLedgerInitialConditions().runWith(Sink.head))
      authorizer = new Authorizer(
        () => java.time.Clock.systemUTC.instant(),
        initialConditions.ledgerId,
        participantId)
      indexService <- JdbcIndex
        .owner(
          ServerRole.ApiServer,
          initialConditions.config,
          domain.LedgerId(initialConditions.ledgerId),
          participantId,
          config.jdbcUrl,
          config.eventsPageSize,
          metrics,
        )
        .map(transformIndexService)
      healthChecks = new HealthChecks(
        "index" -> indexService,
        "read" -> readService,
        "write" -> writeService,
      )
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner()
      apiServicesOwner = new ApiServices.Owner(
        participantId = participantId,
        writeService = writeService,
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
        healthChecks = healthChecks,
        seedService = SeedService(config.seeding),
      )(materializer, executionSequencerFactory, logCtx)
        .map(_.withServices(otherServices))
      apiServer <- new LedgerApiServer(
        apiServicesOwner,
        config.port,
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig.flatMap(_.server),
        AuthorizationInterceptor(authService, executionContext) :: otherInterceptors,
        metrics
      )
    } yield {
      writePortFile(apiServer.port)
      logger.info(
        s"Initialized API server version ${BuildInfo.Version} with ledger-id = ${initialConditions.ledgerId}, port = ${apiServer.port}, dar file = ${config.archiveFiles}")
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
      Files.write(path, Seq(port.toString).asJava)
    }
}
