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
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.server.apiserver.{ApiServer, ApiServices, LedgerApiServer}
import com.digitalasset.platform.index.StandaloneIndexServer.{
  asyncTolerance,
  logger,
  preloadPackages
}
import com.digitalasset.platform.index.config.Config
import com.digitalasset.platform.sandbox.BuildInfo
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.InMemoryPackageStore
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

// Main entry point to start an index server that also hosts the ledger API.
// See v2.ReferenceServer on how it is used.
object StandaloneIndexServer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val asyncTolerance = 30.seconds

  def apply(
      config: Config,
      readService: ReadService,
      writeService: WriteService): StandaloneIndexServer =
    new StandaloneIndexServer(
      "sandbox",
      config,
      readService,
      writeService
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
    writeService: WriteService) {

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
  private def buildAndStartApiServer(infra: Infrastructure): ApiServerState = {
    implicit val mat = infra.materializer
    implicit val ec: ExecutionContext = infra.executionContext
    implicit val mm: MetricsManager = infra.metricsManager

    val packageStore = loadDamlPackages()
    preloadPackages(packageStore)

    val initF = for {
      cond <- readService.getLedgerInitialConditions().runWith(Sink.head)
      indexService <- JdbcIndex(
        readService,
        domain.LedgerId(cond.ledgerId),
        participantId,
        config.jdbcUrl)
    } yield (cond.ledgerId, cond.config.timeModel, indexService)

    val (actualLedgerId, timeModel, indexService) = Try(Await.result(initF, asyncTolerance))
      .fold(t => {
        val msg = "Could not create SandboxIndexAndWriteService"
        logger.error(msg, t)
        sys.error(msg)
      }, identity)

    val apiServer = Await.result(
      LedgerApiServer.create(
        (am: ActorMaterializer, esf: ExecutionSequencerFactory) =>
          ApiServices
            .create(
              writeService,
              indexService,
              StandaloneIndexServer.engine,
              config.timeProvider,
              timeModel,
              SandboxConfig.defaultCommandConfig,
              None)(am, esf),
        config.port,
        config.maxInboundMessageSize,
        None,
        config.tlsConfig.flatMap(_.server)
      ),
      asyncTolerance
    )

    val newState = ApiServerState(
      domain.LedgerId(actualLedgerId),
      apiServer,
      indexService
    )

    logger.info(
      "Initialized index server version {} with ledger-id = {}, port = {}, dar file = {}",
      BuildInfo.Version,
      actualLedgerId,
      newState.port.toString,
      config.archiveFiles
    )

    writePortFile(newState.port)

    newState
  }

  def start(): SandboxState = {
    val actorSystem = ActorSystem(actorSystemName)
    val infrastructure =
      Infrastructure(actorSystem, ActorMaterializer()(actorSystem), MetricsManager(false))
    val apiState = buildAndStartApiServer(infrastructure)

    logger.info("Started Index Server")

    SandboxState(apiState, infrastructure)
  }

  private def writePortFile(port: Int): Unit = {
    config.portFile.foreach { f =>
      val w = new FileWriter(f)
      w.write(s"$port\n")
      w.close()
    }
  }

}
