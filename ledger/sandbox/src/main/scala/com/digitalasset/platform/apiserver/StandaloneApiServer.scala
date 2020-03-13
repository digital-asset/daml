// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.io.File
import java.nio.file.Files
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService, SeedService, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.ledger.api.auth.{AuthService, Authorizer}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.health.HealthChecks
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.apiserver.StandaloneApiServer._
import com.digitalasset.platform.configuration.{
  BuildInfo,
  CommandConfiguration,
  PartyConfiguration,
  SubmissionConfiguration
}
import com.digitalasset.platform.index.JdbcIndex
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.ports.Port
import com.digitalasset.resources.{Resource, ResourceOwner}
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
    submissionConfig: SubmissionConfiguration,
    readService: ReadService,
    writeService: WriteService,
    authService: AuthService,
    metrics: MetricRegistry,
    timeServiceBackend: Option[TimeServiceBackend] = None,
    seeding: Seeding,
    otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
    otherInterceptors: List[ServerInterceptor] = List.empty,
    engine: Engine = sharedEngine // allows sharing DAML engine with DAML-on-X participant
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
      indexService <- JdbcIndex.owner(
        initialConditions.config.timeModel,
        domain.LedgerId(initialConditions.ledgerId),
        participantId,
        config.jdbcUrl,
        metrics,
      )
      healthChecks = new HealthChecks(
        "index" -> indexService,
        "read" -> readService,
        "write" -> writeService,
      )
      apiServer <- new LedgerApiServer(
        (mat: Materializer, esf: ExecutionSequencerFactory) => {
          ApiServices
            .create(
              participantId = participantId,
              writeService = writeService,
              indexService = indexService,
              authorizer = authorizer,
              engine = engine,
              timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
              defaultLedgerConfiguration = initialConditions.config,
              commandConfig = commandConfig,
              partyConfig = partyConfig,
              submissionConfig = submissionConfig,
              optTimeServiceBackend = timeServiceBackend,
              metrics = metrics,
              healthChecks = healthChecks,
              seedService = Some(SeedService(seeding)),
            )(mat, esf, logCtx)
            .map(_.withServices(otherServices))
        },
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

  // if requested, initialize the ledger state with the given scenario
  private def preloadPackages(packageContainer: InMemoryPackageStore): Unit = {
    // [[ScenarioLoader]] needs all the packages to be already compiled --
    // make sure that that's the case
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

object StandaloneApiServer {
  private val sharedEngine = Engine()
}
