// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.io.{File, FileWriter}
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService, WriteService}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.server.apiserver.{ApiServer, ApiServices, LedgerApiServer}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.index.StandaloneIndexServer.{asyncTolerance, preloadPackages}
import com.digitalasset.platform.index.config.Config
import com.digitalasset.platform.sandbox.BuildInfo
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.InMemoryPackageStore

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

// Main entry point to start an index server that also hosts the ledger API.
// See v2.ReferenceServer on how it is used.
object StandaloneIndexServer {
  private val asyncTolerance = 30.seconds

  def apply(
      config: Config,
      readService: ReadService,
      writeService: WriteService,
      authService: AuthService,
      loggerFactory: NamedLoggerFactory): StandaloneIndexServer =
    new StandaloneIndexServer(
      "index",
      config,
      readService,
      writeService,
      authService,
      loggerFactory
    )

  private val engine = Engine()

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
    }
  }
}

class StandaloneIndexServer(
    actorSystemName: String,
    config: Config,
    readService: ReadService,
    writeService: WriteService,
    authService: AuthService,
    loggerFactory: NamedLoggerFactory) {
  private val logger = loggerFactory.getLogger(this.getClass)

  // Name of this participant,
  val participantId: ParticipantId = config.participantId

  case class ApiServerState(
      ledgerId: LedgerId,
      apiServer: ApiServer,
      indexAndWriteService: AutoCloseable
  ) extends AutoCloseable {
    def port: Int = apiServer.port

    override def close: Unit = {
      apiServer.close() //fully tear down the old server.
      indexAndWriteService.close()
    }
  }

  case class Infrastructure(
      actorSystem: ActorSystem,
      materializer: ActorMaterializer,
      metricsManager: MetricsManager)
      extends AutoCloseable {
    def executionContext: ExecutionContext = materializer.executionContext

    override def close: Unit = {
      materializer.shutdown()
      Await.result(actorSystem.terminate(), asyncTolerance)
      metricsManager.close()
    }
  }

  case class SandboxState(apiServerState: ApiServerState, infra: Infrastructure)
      extends AutoCloseable {
    override def close(): Unit = {
      apiServerState.close()
      infra.close()
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

  @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
  private def buildAndStartApiServer(infra: Infrastructure)(
      implicit ec: ExecutionContext): Future[ApiServerState] = {
    implicit val mat = infra.materializer
    implicit val mm: MetricsManager = infra.metricsManager

    val packageStore = loadDamlPackages()
    preloadPackages(packageStore)

    for {
      cond <- readService.getLedgerInitialConditions().runWith(Sink.head)
      indexService <- JdbcIndex(
        readService,
        domain.LedgerId(cond.ledgerId),
        participantId,
        config.jdbcUrl,
        loggerFactory)
      apiServer <- LedgerApiServer.create(
        (am: ActorMaterializer, esf: ExecutionSequencerFactory) =>
          ApiServices
            .create(
              writeService,
              indexService,
              authService,
              StandaloneIndexServer.engine,
              config.timeProvider,
              cond.config.timeModel,
              SandboxConfig.defaultCommandConfig,
              None,
              loggerFactory
            )(am, esf),
        config.port,
        config.maxInboundMessageSize,
        None,
        loggerFactory,
        config.tlsConfig.flatMap(_.server),
        List(AuthorizationInterceptor(authService, ec))
      )
      apiServerState = ApiServerState(
        domain.LedgerId(cond.ledgerId),
        apiServer,
        indexService
      )
      _ = logger.info(
        "Initialized index server version {} with ledger-id = {}, port = {}, dar file = {}",
        BuildInfo.Version,
        cond.ledgerId,
        apiServerState.port.toString,
        config.archiveFiles
      )

      _ = writePortFile(apiServerState.port)
    } yield apiServerState
  }

  def start(): Future[SandboxState] = {
    val actorSystem = ActorSystem(actorSystemName)
    val infrastructure =
      Infrastructure(actorSystem, ActorMaterializer()(actorSystem), MetricsManager(false))
    implicit val ec: ExecutionContext = infrastructure.executionContext
    val apiState = buildAndStartApiServer(infrastructure)

    logger.info("Started Index Server")

    apiState.map(SandboxState(_, infrastructure))
  }

  private def writePortFile(port: Int): Unit = {
    config.portFile.foreach { f =>
      val w = new FileWriter(f)
      w.write(s"$port\n")
      w.close()
    }
  }

}
