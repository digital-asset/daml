// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.nio.file.Files
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.participant.state.{v1 => ParticipantState}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard, Authorizer}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.health.HealthChecks
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.apiserver.{
  ApiServer,
  ApiServices,
  LedgerApiServer,
  TimeServiceBackend
}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer._
import com.digitalasset.platform.sandbox.banner.Banner
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.metrics.MetricsReporting
import com.digitalasset.platform.sandbox.services.SandboxResetService
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger._
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.digitalasset.platform.sandbox.stores.{
  InMemoryActiveLedgerState,
  InMemoryPackageStore,
  SandboxIndexAndWriteService
}
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

object SandboxServer {
  private val ActorSystemName = "sandbox"
  private val AsyncTolerance = 30.seconds

  private val logger = ContextualizedLogger.get(this.getClass)

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

  private final case class SandboxState(
      // nested resource so we can release it independently when restarting
      apiServer: Resource[ApiServer],
      packageStore: InMemoryPackageStore,
      materializer: Materializer,
  ) {
    val executionContext: ExecutionContext = materializer.executionContext

    def port: Future[Int] = {
      apiServer.asFuture.map(_.port)(executionContext)
    }
  }
}

final class SandboxServer(config: => SandboxConfig) extends AutoCloseable {

  // Name of this participant
  // TODO: Pass this info in command-line (See issue #2025)
  val participantId: ParticipantId = Ref.LedgerString.assertFromString("sandbox-participant")

  private val authService: AuthService = config.authService.getOrElse(AuthServiceWildcard)

  private val metrics = new MetricRegistry

  @volatile private var sandboxState: Resource[SandboxState] = _

  sandboxState = {
    implicit val executionContext: ExecutionContext = DirectExecutionContext
    for {
      _ <- ResourceOwner
        .forCloseable(() => new MetricsReporting(metrics, getClass.getPackage.getName))
        .acquire()
      state <- start()
    } yield state
  }

  def failure: Option[Throwable] =
    Await
      .result(
        sandboxState.asFuture.transformWith(Future.successful)(DirectExecutionContext),
        AsyncTolerance)
      .failed
      .toOption

  def port: Int =
    Await.result(portF(DirectExecutionContext), AsyncTolerance)

  def portF(implicit executionContext: ExecutionContext): Future[Int] =
    sandboxState.asFuture.flatMap(_.apiServer.asFuture).map(_.port)

  /** the reset service is special, since it triggers a server shutdown */
  private def resetService(
      ledgerId: LedgerId,
      authorizer: Authorizer,
      executionContext: ExecutionContext,
  )(implicit logCtx: LoggingContext): SandboxResetService =
    new SandboxResetService(
      ledgerId,
      () => executionContext,
      () => resetAndRestartServer()(executionContext),
      authorizer,
    )

  def resetAndRestartServer()(implicit executionContext: ExecutionContext): Future[Unit] = {
    val apiServicesClosed =
      sandboxState.asFuture.flatMap(_.apiServer.asFuture).flatMap(_.servicesClosed())

    // Need to run this async otherwise the callback kills the server under the in-flight reset service request!
    // TODO: eliminate the state mutation somehow
    sandboxState = for {
      state <- sandboxState
      currentPort <- ResourceOwner.forFuture(() => state.port).acquire()
      _ <- ResourceOwner.forFuture(() => state.apiServer.release()).acquire()
    } yield
      state.copy(
        apiServer = buildAndStartApiServer(
          state.materializer,
          state.packageStore,
          SqlStartMode.AlwaysReset,
          Some(currentPort),
        ))

    // waits for the services to be closed, so we can guarantee that future API calls after finishing the reset will never be handled by the old one
    apiServicesClosed
  }

  private def buildAndStartApiServer(
      materializer: Materializer,
      packageStore: InMemoryPackageStore,
      startMode: SqlStartMode,
      currentPort: Option[Int],
  ): Resource[ApiServer] = {
    implicit val _materializer: Materializer = materializer
    implicit val actorSystem: ActorSystem = materializer.system
    implicit val executionContext: ExecutionContext = materializer.executionContext

    val ledgerId = config.ledgerIdMode match {
      case LedgerIdMode.Static(id) => id
      case LedgerIdMode.Dynamic() => LedgerIdGenerator.generateRandomId()
    }

    val defaultConfiguration = ParticipantState.Configuration(0, config.timeModel)

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

    newLoggingContext(logging.participantId(participantId)) { implicit logCtx =>
      val (ledgerType, indexAndWriteServiceResourceOwner) = config.jdbcUrl match {
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
            packageStore,
            metrics,
          )

        case None =>
          "in-memory" -> SandboxIndexAndWriteService.inMemory(
            ledgerId,
            participantId,
            config.timeModel,
            timeProvider,
            acs,
            ledgerEntries,
            packageStore,
            metrics,
          )
      }

      for {
        indexAndWriteService <- indexAndWriteServiceResourceOwner.acquire()
        authorizer = new Authorizer(
          () => java.time.Clock.systemUTC.instant(),
          LedgerId.unwrap(ledgerId),
          participantId)
        healthChecks = new HealthChecks(
          "index" -> indexAndWriteService.indexService,
          "write" -> indexAndWriteService.writeService,
        )
        // NOTE: Re-use the same port after reset.
        apiServer <- new LedgerApiServer(
          (mat: Materializer, esf: ExecutionSequencerFactory) =>
            ApiServices
              .create(
                indexAndWriteService.writeService,
                indexAndWriteService.indexService,
                authorizer,
                SandboxServer.engine,
                timeProvider,
                defaultConfiguration,
                config.commandConfig,
                timeServiceBackendO
                  .map(TimeServiceBackend.withObserver(_, indexAndWriteService.publishHeartbeat)),
                metrics,
                healthChecks,
              )(mat, esf, logCtx)
              .map(_.withServices(List(resetService(ledgerId, authorizer, executionContext)))),
          currentPort.getOrElse(config.port),
          config.maxInboundMessageSize,
          config.address,
          config.tlsConfig.flatMap(_.server),
          List(
            AuthorizationInterceptor(authService, executionContext),
            resetService(ledgerId, authorizer, executionContext),
          ),
          metrics
        ).acquire()
        _ <- ResourceOwner.forFuture(() => writePortFile(apiServer.port)).acquire()
      } yield {
        Banner.show(Console.out)
        logger.withoutContext.info(
          "Initialized sandbox version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}",
          BuildInfo.Version,
          ledgerId,
          apiServer.port.toString,
          config.damlPackages,
          config.timeProviderType,
          ledgerType,
          authService.getClass.getSimpleName
        )
        apiServer
      }

    }

  }

  private def start()(implicit executionContext: ExecutionContext): Resource[SandboxState] = {
    val packageStore = loadDamlPackages()
    for {
      actorSystem <- AkkaResourceOwner.forActorSystem(() => ActorSystem(ActorSystemName)).acquire()
      materializer <- AkkaResourceOwner.forMaterializer(() => Materializer(actorSystem)).acquire()
    } yield {
      val apiServerResource = buildAndStartApiServer(
        materializer,
        packageStore,
        SqlStartMode.ContinueIfExists,
        currentPort = None,
      )
      SandboxState(apiServerResource, packageStore, materializer)
    }
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

  override def close(): Unit = {
    Await.result(
      sandboxState.flatMap(_.apiServer)(DirectExecutionContext).release(),
      AsyncTolerance)
  }

  private def writePortFile(port: Int)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] =
    config.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.successful(()))
}
