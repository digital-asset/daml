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
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService, WriteService}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.ledger.api.auth.{AuthService, Authorizer}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.health.HealthChecks
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.apiserver.StandaloneApiServer._
import com.digitalasset.platform.configuration.{BuildInfo, CommandConfiguration}
import com.digitalasset.platform.index.JdbcIndex
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

// Main entry point to start an index server that also hosts the ledger API.
// See v2.ReferenceServer on how it is used.
final class StandaloneApiServer(
    config: ApiServerConfig,
    readService: ReadService,
    writeService: WriteService,
    authService: AuthService,
    metrics: MetricRegistry,
    engine: Engine = sharedEngine, // allows sharing DAML engine with DAML-on-X participant
    timeServiceBackendO: Option[TimeServiceBackend] = None,
)(implicit logCtx: LoggingContext)
    extends ResourceOwner[Unit] {

  private val logger = ContextualizedLogger.get(this.getClass)

  // Name of this participant,
  val participantId: ParticipantId = config.participantId

  override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] = {
    buildAndStartApiServer().map { _ =>
      logger.info("Started Index Server")
      ()
    }
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

  private def buildAndStartApiServer()(implicit ec: ExecutionContext): Resource[ApiServer] = {
    val packageStore = loadDamlPackages()
    preloadPackages(packageStore)

    for {
      actorSystem <- AkkaResourceOwner.forActorSystem(() => ActorSystem(actorSystemName)).acquire()
      materializer <- AkkaResourceOwner.forMaterializer(() => Materializer(actorSystem)).acquire()
      initialConditions <- Resource.fromFuture(
        readService.getLedgerInitialConditions().runWith(Sink.head)(materializer))
      authorizer = new Authorizer(
        () => java.time.Clock.systemUTC.instant(),
        initialConditions.ledgerId,
        participantId)
      indexService <- JdbcIndex(
        initialConditions.config.timeModel,
        domain.LedgerId(initialConditions.ledgerId),
        participantId,
        config.jdbcUrl,
        metrics,
      )(materializer, logCtx)
      healthChecks = new HealthChecks(
        "index" -> indexService,
        "read" -> readService,
        "write" -> writeService,
      )
      apiServer <- new LedgerApiServer(
        (mat: Materializer, esf: ExecutionSequencerFactory) =>
          ApiServices
            .create(
              participantId = participantId,
              writeService = writeService,
              indexService = indexService,
              authorizer = authorizer,
              engine = engine,
              timeProvider = config.timeProvider,
              defaultLedgerConfiguration = initialConditions.config,
              commandConfig = CommandConfiguration.default,
              optTimeServiceBackend = timeServiceBackendO,
              metrics = metrics,
              healthChecks = healthChecks,
              seedService = None,
            )(mat, esf, logCtx),
        config.port,
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig.flatMap(_.server),
        List(AuthorizationInterceptor(authService, ec)),
        metrics
      )(actorSystem, materializer, logCtx).acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port))
    } yield {
      logger.info(
        s"Initialized index server version ${BuildInfo.Version} with ledger-id = ${initialConditions.ledgerId}, port = ${apiServer.port}, dar file = ${config.archiveFiles}")
      apiServer
    }
  }

  private def writePortFile(port: Int)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] =
    config.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.successful(()))
}

object StandaloneApiServer {
  private val actorSystemName = "index"

  private val sharedEngine = Engine()
}
