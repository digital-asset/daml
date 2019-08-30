// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.{File, FileWriter}
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.server.apiserver.{ApiServer, ApiServices, LedgerApiServer}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer.{asyncTolerance, createInitialState, logger}
import com.digitalasset.platform.sandbox.banner.Banner
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.services.SandboxResetService
import com.digitalasset.platform.sandbox.stores.{
  InMemoryActiveLedgerState,
  InMemoryPackageStore,
  SandboxIndexAndWriteService
}
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger._
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.digitalasset.platform.server.services.testing.TimeServiceBackend
import com.digitalasset.platform.services.time.TimeProviderType
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object SandboxServer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val asyncTolerance = 30.seconds

  def apply(config: => SandboxConfig): SandboxServer =
    new SandboxServer(
      "sandbox",
      config
    )

  // We memoize the engine between resets so we avoid the expensive
  // repeated validation of the sames packages after each reset
  private val engine = Engine()

  // if requested, initialize the ledger state with the given scenario
  private def createInitialState(config: SandboxConfig, packageStore: InMemoryPackageStore)
    : (InMemoryActiveLedgerState, ImmArray[LedgerEntryOrBump], Option[Instant]) = {
    // [[ScenarioLoader]] needs all the packages to be already compiled --
    // make sure that that's the case
    if (config.eagerPackageLoading || config.scenario.nonEmpty) {
      for (pkgId <- packageStore.listLfPackagesSync().keys) {
        val pkg = packageStore.getLfPackageSync(pkgId).get
        engine
          .preloadPackage(pkgId, pkg)
          .consume(
            { _ =>
              sys.error("Unexpected request of contract")
            },
            packageStore.getLfPackageSync, { _ =>
              sys.error("Unexpected request of contract key")
            }
          )
      }
    }
    config.scenario match {
      case None => (InMemoryActiveLedgerState.empty, ImmArray.empty, None)
      case Some(scenario) =>
        val (acs, records, ledgerTime) =
          ScenarioLoader.fromScenario(packageStore, engine.compiledPackages(), scenario)
        (acs, records, Some(ledgerTime))
    }
  }
}

class SandboxServer(actorSystemName: String, config: => SandboxConfig) extends AutoCloseable {

  // Name of this participant
  // TODO: Pass this info in command-line (See issue #2025)
  val participantId: ParticipantId = Ref.LedgerString.assertFromString("sandbox-participant")

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

  @volatile private var sandboxState: SandboxState = _

  case class SandboxState(
      apiServerState: ApiServerState,
      infra: Infrastructure,
      packageStore: InMemoryPackageStore)
      extends AutoCloseable {
    override def close(): Unit = {
      // FIXME: extra close - when closed during reset close is called on already closed service causing an exception!
      apiServerState.close()
      infra.close()
    }

    def resetAndRestartServer(): Future[Unit] = {
      implicit val ec: ExecutionContext = sandboxState.infra.executionContext
      val apiServicesClosed = apiServerState.apiServer.servicesClosed()
      //need to run this async otherwise the callback kills the server under the in-flight reset service request!

      Future {
        apiServerState.close // fully tear down the old server
        //TODO: eliminate the state mutation somehow
        //yes, it's horrible that we mutate the state here, but believe me, it's still an improvement to what we had before!
        sandboxState = copy(
          apiServerState =
            buildAndStartApiServer(infra, sandboxState.packageStore, SqlStartMode.AlwaysReset))
      }(infra.executionContext)

      // waits for the services to be closed, so we can guarantee that future API calls after finishing the reset will never be handled by the old one
      apiServicesClosed
    }

  }

  def port: Int = sandboxState.apiServerState.port

  /** the reset service is special, since it triggers a server shutdown */
  private def resetService(ledgerId: LedgerId): SandboxResetService = new SandboxResetService(
    ledgerId,
    () => sandboxState.infra.executionContext,
    () => sandboxState.resetAndRestartServer()
  )

  sandboxState = start()

  @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
  private def buildAndStartApiServer(
      infra: Infrastructure,
      packageStore: InMemoryPackageStore,
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists): ApiServerState = {
    implicit val mat = infra.materializer
    implicit val ec: ExecutionContext = infra.executionContext
    implicit val mm: MetricsManager = infra.metricsManager

    val ledgerId = config.ledgerIdMode match {
      case LedgerIdMode.Static(id) => id
      case LedgerIdMode.Dynamic() => LedgerIdGenerator.generateRandomId()
    }

    val (acs, ledgerEntries, mbLedgerTime) = createInitialState(config, packageStore)

    val (timeProvider, timeServiceBackendO: Option[TimeServiceBackend]) =
      (mbLedgerTime, config.timeProviderType) match {
        case (None, TimeProviderType.WallClock) => (TimeProvider.UTC, None)
        case (ledgerTime, _) =>
          val ts = TimeServiceBackend.simple(
            ledgerTime.getOrElse(Instant.EPOCH),
            config.timeProviderType == TimeProviderType.StaticAllowBackwards)
          (ts, Some(ts))
      }

    val (ledgerType, indexAndWriteServiceF) = config.jdbcUrl match {
      case Some(jdbcUrl) =>
        "postgres" -> SandboxIndexAndWriteService.postgres(
          ledgerId,
          participantId,
          jdbcUrl,
          config.timeModel,
          timeProvider,
          acs,
          ledgerEntries,
          startMode,
          config.commandConfig.maxCommandsInFlight * 2, // we can get commands directly as well on the submission service
          packageStore
        )

      case None =>
        "in-memory" -> Future.successful(
          SandboxIndexAndWriteService.inMemory(
            ledgerId,
            participantId,
            config.timeModel,
            timeProvider,
            acs,
            ledgerEntries,
            packageStore
          ))
    }

    val indexAndWriteService = Try(Await.result(indexAndWriteServiceF, asyncTolerance))
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
              indexAndWriteService.writeService,
              indexAndWriteService.indexService,
              SandboxServer.engine,
              timeProvider,
              config.timeModel,
              config.commandConfig,
              timeServiceBackendO
                .map(
                  TimeServiceBackend.withObserver(
                    _,
                    indexAndWriteService.publishHeartbeat
                  ))
            )(am, esf)
            .map(_.withServices(List(resetService(ledgerId)))),
        // NOTE(JM): Re-use the same port after reset.
        Option(sandboxState).fold(config.port)(_.apiServerState.port),
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig.flatMap(_.server),
        List(resetService(ledgerId))
      ),
      asyncTolerance
    )

    val newState = ApiServerState(
      ledgerId,
      apiServer,
      indexAndWriteService
    )

    Banner.show(Console.out)
    logger.info(
      "Initialized sandbox version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, daml-engine = {}",
      BuildInfo.Version,
      ledgerId,
      newState.port.toString,
      config.damlPackages,
      config.timeProviderType,
      ledgerType
    )

    writePortFile(newState.port)

    newState
  }

  private def start(): SandboxState = {
    val actorSystem = ActorSystem(actorSystemName)
    val infrastructure =
      Infrastructure(actorSystem, ActorMaterializer()(actorSystem), MetricsManager())
    val packageStore = loadDamlPackages
    val apiState = buildAndStartApiServer(infrastructure, packageStore)
    SandboxState(apiState, infrastructure, packageStore)
  }

  private def loadDamlPackages(): InMemoryPackageStore = {
    // TODO is it sensible to have all the initial packages to be known since the epoch?
    config.damlPackages
      .foldLeft[Either[(String, File), InMemoryPackageStore]](Right(InMemoryPackageStore.empty)) {
        case (storeE, f) =>
          storeE.flatMap(_.withDarFile(Instant.EPOCH, None, f).left.map(_ -> f))

      }
      .fold({ case (err, file) => sys.error(s"Could not load package $file: $err") }, identity)
  }

  override def close(): Unit = sandboxState.close()

  private def writePortFile(port: Int): Unit = {
    config.portFile.foreach { f =>
      val w = new FileWriter(f)
      w.write(s"$port\n")
      w.close()
    }
  }

}
