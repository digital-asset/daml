// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.nio.file.Files
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{ParticipantId, SeedService}
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
import com.digitalasset.platform.configuration.BuildInfo
import com.digitalasset.platform.packages.InMemoryPackageStore
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
  SandboxIndexAndWriteService
}
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object SandboxServer {
  private val ActorSystemName = "sandbox"
  private val AsyncTolerance = 30.seconds

  private val logger = ContextualizedLogger.get(this.getClass)

  // We memoize the engine between resets so we avoid the expensive
  // repeated validation of the sames packages after each reset
  private val engine = Engine()

  def owner(config: SandboxConfig): ResourceOwner[SandboxServer] =
    for {
      metrics <- ResourceOwner.successful(new MetricRegistry)
      _ <- ResourceOwner
        .forCloseable(() => new MetricsReporting(metrics, classOf[SandboxServer].getName))
      actorSystem <- AkkaResourceOwner.forActorSystem(() => ActorSystem(ActorSystemName))
      materializer <- AkkaResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- ResourceOwner
        .forTryCloseable(() => Try(new SandboxServer(config, materializer, metrics)))
      // Wait for the API server to start.
      _ <- new ResourceOwner[Unit] {
        override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] =
          // We use the Future rather than the Resource to avoid holding onto the API server.
          // Otherwise, we cause a memory leak upon reset.
          Resource.fromFuture(server.apiServer.map(_ => ()))
      }
    } yield server

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

  final class SandboxState(
      materializer: Materializer,
      metrics: MetricRegistry,
      packageStore: InMemoryPackageStore,
      // nested resource so we can release it independently when restarting
      apiServerResource: Resource[ApiServer],
  ) {
    def port(implicit executionContext: ExecutionContext): Future[Int] =
      apiServer.map(_.port)

    private[SandboxServer] def apiServer(
        implicit executionContext: ExecutionContext
    ): Future[ApiServer] =
      apiServerResource.asFuture

    private[SandboxServer] def reset(
        newApiServer: (
            Materializer,
            MetricRegistry,
            InMemoryPackageStore,
            Int // port number
        ) => Resource[ApiServer]
    )(implicit executionContext: ExecutionContext): Future[SandboxState] =
      for {
        currentPort <- port
        _ <- release()
      } yield
        new SandboxState(
          materializer,
          metrics,
          packageStore,
          newApiServer(materializer, metrics, packageStore, currentPort))

    def release()(implicit executionContext: ExecutionContext): Future[Unit] =
      apiServerResource.release()
  }
}

final class SandboxServer(
    config: SandboxConfig,
    materializer: Materializer,
    metrics: MetricRegistry,
) extends AutoCloseable {

  // Only used for testing.
  def this(config: SandboxConfig, materializer: Materializer) =
    this(config, materializer, new MetricRegistry)

  // Name of this participant
  // TODO: Pass this info in command-line (See issue #2025)
  val participantId: ParticipantId = Ref.ParticipantId.assertFromString("sandbox-participant")

  private val authService: AuthService = config.authService.getOrElse(AuthServiceWildcard)

  // We store a Future rather than a Resource to avoid keeping old resources around after a reset.
  // It's package-private so we can test that we drop the reference properly in ResetServiceIT.
  @volatile
  private[sandbox] var sandboxState: Future[SandboxState] = start()

  private def apiServer(implicit executionContext: ExecutionContext): Future[ApiServer] =
    sandboxState.flatMap(_.apiServer)

  // Only used in testing; hopefully we can get rid of it soon.
  def port: Int =
    Await.result(portF(DirectExecutionContext), AsyncTolerance)

  def portF(implicit executionContext: ExecutionContext): Future[Int] =
    apiServer.map(_.port)

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
    val apiServicesClosed = apiServer.flatMap(_.servicesClosed())

    // Need to run this async otherwise the callback kills the server under the in-flight reset service request!
    // TODO: eliminate the state mutation somehow
    sandboxState = sandboxState.flatMap(
      _.reset(
        (materializer, metrics, packageStore, port) =>
          buildAndStartApiServer(
            materializer,
            metrics,
            packageStore,
            SqlStartMode.AlwaysReset,
            Some(port),
        )))

    // waits for the services to be closed, so we can guarantee that future API calls after finishing the reset will never be handled by the old one
    apiServicesClosed
  }

  private def buildAndStartApiServer(
      materializer: Materializer,
      metrics: MetricRegistry,
      packageStore: InMemoryPackageStore,
      startMode: SqlStartMode,
      currentPort: Option[Int],
  ): Resource[ApiServer] = {
    implicit val _materializer: Materializer = materializer
    implicit val actorSystem: ActorSystem = materializer.system
    implicit val executionContext: ExecutionContext = materializer.executionContext

    val defaultConfiguration = ParticipantState.Configuration(0, config.timeModel)

    val (acs, ledgerEntries, mbLedgerTime) = createInitialState(config, packageStore)

    val timeProviderType = config.timeProviderType.getOrElse(TimeProviderType.Static)
    val (timeProvider, timeServiceBackendO: Option[TimeServiceBackend]) =
      (mbLedgerTime, timeProviderType) match {
        case (None, TimeProviderType.WallClock) => (TimeProvider.UTC, None)
        case (ledgerTime, _) =>
          val ts = TimeServiceBackend.simple(ledgerTime.getOrElse(Instant.EPOCH))
          (ts, Some(ts))
      }

    newLoggingContext(logging.participantId(participantId)) { implicit logCtx =>
      val (ledgerType, indexAndWriteServiceResourceOwner) = config.jdbcUrl match {
        case Some(jdbcUrl) =>
          "postgres" -> SandboxIndexAndWriteService.postgres(
            config.ledgerIdMode,
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
            config.ledgerIdMode,
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
        ledgerId <- Resource.fromFuture(indexAndWriteService.indexService.getLedgerId())
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
                participantId = participantId,
                writeService = indexAndWriteService.writeService,
                indexService = indexAndWriteService.indexService,
                authorizer = authorizer,
                engine = SandboxServer.engine,
                timeProvider = timeProvider,
                defaultLedgerConfiguration = defaultConfiguration,
                commandConfig = config.commandConfig,
                submissionConfig = config.submissionConfig,
                optTimeServiceBackend = timeServiceBackendO
                  .map(TimeServiceBackend.withObserver(_, indexAndWriteService.publishHeartbeat)),
                metrics = metrics,
                healthChecks = healthChecks,
                seedService = config.seeding.map(SeedService(_)),
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
        _ <- Resource.fromFuture(writePortFile(apiServer.port))
      } yield {
        Banner.show(Console.out)
        logger.withoutContext.info(
          "Initialized sandbox version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}",
          BuildInfo.Version,
          ledgerId,
          apiServer.port.toString,
          config.damlPackages,
          timeProviderType.description,
          ledgerType,
          authService.getClass.getSimpleName
        )
        if (config.seeding.contains(SeedService.Seeding.Weak))
          logger.warn("Contract id seeding uses a weak seed. DO NOT USE IN PRODUCTION.")
        apiServer
      }
    }
  }

  private def start(): Future[SandboxState] = {
    val packageStore = loadDamlPackages()
    val apiServerResource = buildAndStartApiServer(
      materializer,
      metrics,
      packageStore,
      SqlStartMode.ContinueIfExists,
      currentPort = None,
    )
    Future.successful(new SandboxState(materializer, metrics, packageStore, apiServerResource))
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
    implicit val executionContext: ExecutionContext = DirectExecutionContext
    Await.result(sandboxState.flatMap(_.release()), AsyncTolerance)
  }

  private def writePortFile(port: Int)(implicit executionContext: ExecutionContext): Future[Unit] =
    config.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.successful(()))
}
