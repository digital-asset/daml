// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.io.File
import java.nio.file.Files
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard, Authorizer}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.participant.state.v1.metrics.TimedWriteService
import com.daml.ledger.participant.state.v1.{ParticipantId, SeedService}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.{
  LegacyTransactionCommitter,
  StandardTransactionCommitter,
  TransactionCommitter
}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver._
import com.daml.platform.configuration.PartyConfiguration
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.SandboxServer._
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.metrics.MetricsReporting
import com.daml.platform.sandbox.services.SandboxResetService
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger._
import com.daml.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.daml.platform.sandbox.stores.{InMemoryActiveLedgerState, SandboxIndexAndWriteService}
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.daml.resources.akka.AkkaResourceOwner
import com.daml.resources.{Resource, ResourceOwner}

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
      metrics <- new MetricsReporting(
        classOf[SandboxServer].getName,
        config.metricsReporter,
        config.metricsReportingInterval,
      )
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

  final class SandboxState(
      materializer: Materializer,
      metrics: Metrics,
      packageStore: InMemoryPackageStore,
      // nested resource so we can release it independently when restarting
      apiServerResource: Resource[ApiServer],
  ) {
    def port(implicit executionContext: ExecutionContext): Future[Port] =
      apiServer.map(_.port)

    private[SandboxServer] def apiServer(
        implicit executionContext: ExecutionContext
    ): Future[ApiServer] =
      apiServerResource.asFuture

    private[SandboxServer] def reset(
        newApiServer: (
            Materializer,
            Metrics,
            InMemoryPackageStore,
            Port,
        ) => Resource[ApiServer]
    )(implicit executionContext: ExecutionContext): Future[SandboxState] =
      for {
        currentPort <- port
        _ <- release()
        replacementApiServer = newApiServer(materializer, metrics, packageStore, currentPort)
        _ <- replacementApiServer.asFuture
      } yield new SandboxState(materializer, metrics, packageStore, replacementApiServer)

    def release()(implicit executionContext: ExecutionContext): Future[Unit] =
      apiServerResource.release()
  }
}

final class SandboxServer(
    config: SandboxConfig,
    materializer: Materializer,
    metrics: Metrics,
) extends AutoCloseable {

  // Only used for testing.
  def this(config: SandboxConfig, materializer: Materializer) =
    this(config, materializer, new Metrics(new MetricRegistry))

  // Name of this participant
  // TODO: Pass this info in command-line (See issue #2025)
  val participantId: ParticipantId = Ref.ParticipantId.assertFromString("sandbox-participant")

  private val authService: AuthService = config.authService.getOrElse(AuthServiceWildcard)
  private val seedingService = SeedService(config.seeding.getOrElse(SeedService.Seeding.Weak))

  // We store a Future rather than a Resource to avoid keeping old resources around after a reset.
  // It's package-private so we can test that we drop the reference properly in ResetServiceIT.
  @volatile
  private[sandbox] var sandboxState: Future[SandboxState] = start()

  private def apiServer(implicit executionContext: ExecutionContext): Future[ApiServer] =
    sandboxState.flatMap(_.apiServer)

  // Only used in testing; hopefully we can get rid of it soon.
  def port: Port =
    Await.result(portF(DirectExecutionContext), AsyncTolerance)

  def portF(implicit executionContext: ExecutionContext): Future[Port] =
    apiServer.map(_.port)

  def resetAndRestartServer()(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[Unit] = {
    val apiServicesClosed = apiServer.flatMap(_.servicesClosed())

    // TODO: eliminate the state mutation somehow
    sandboxState = sandboxState.flatMap(
      _.reset(
        (materializer, metrics, packageStore, port) =>
          buildAndStartApiServer(
            materializer = materializer,
            metrics = metrics,
            packageStore = packageStore,
            startMode = SqlStartMode.AlwaysReset,
            currentPort = Some(port),
        )))

    // Wait for the services to be closed, so we can guarantee that future API calls after finishing
    // the reset will never be handled by the old one.
    apiServicesClosed
  }

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
          ScenarioLoader.fromScenario(
            packageStore,
            engine.compiledPackages(),
            scenario,
            seedingService.nextSeed(),
          )
        (acs, records, Some(ledgerTime))
    }
  }

  private def buildAndStartApiServer(
      materializer: Materializer,
      metrics: Metrics,
      packageStore: InMemoryPackageStore,
      startMode: SqlStartMode,
      currentPort: Option[Port],
  )(implicit logCtx: LoggingContext): Resource[ApiServer] = {
    implicit val _materializer: Materializer = materializer
    implicit val actorSystem: ActorSystem = materializer.system
    implicit val executionContext: ExecutionContext = materializer.executionContext

    val defaultConfiguration = config.ledgerConfig.initialConfiguration

    val (acs, ledgerEntries, mbLedgerTime) = createInitialState(config, packageStore)

    val timeProviderType = config.timeProviderType.getOrElse(SandboxConfig.DefaultTimeProviderType)
    val (timeProvider, timeServiceBackendO: Option[TimeServiceBackend]) =
      (mbLedgerTime, timeProviderType) match {
        case (None, TimeProviderType.WallClock) => (TimeProvider.UTC, None)
        case (ledgerTime, _) =>
          val ts = TimeServiceBackend.simple(ledgerTime.getOrElse(Instant.EPOCH))
          (ts, Some(ts))
      }

    val transactionCommitter =
      config.seeding
        .fold[TransactionCommitter](LegacyTransactionCommitter)(_ => StandardTransactionCommitter)

    val (ledgerType, indexAndWriteServiceResourceOwner) = config.jdbcUrl match {
      case Some(jdbcUrl) =>
        "postgres" -> SandboxIndexAndWriteService.postgres(
          config.ledgerIdMode,
          participantId,
          jdbcUrl,
          defaultConfiguration,
          timeProvider,
          acs,
          ledgerEntries,
          startMode,
          config.commandConfig.maxParallelSubmissions,
          transactionCommitter,
          packageStore,
          config.eventsPageSize,
          metrics,
        )

      case None =>
        "in-memory" -> SandboxIndexAndWriteService.inMemory(
          config.ledgerIdMode,
          participantId,
          defaultConfiguration,
          timeProvider,
          acs,
          ledgerEntries,
          transactionCommitter,
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
      // the reset service is special, since it triggers a server shutdown
      resetService = new SandboxResetService(
        ledgerId,
        () => resetAndRestartServer(),
        authorizer,
      )
      ledgerConfiguration = config.ledgerConfig
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner().acquire()
      apiServicesOwner = new ApiServices.Owner(
        participantId = participantId,
        writeService = new TimedWriteService(indexAndWriteService.writeService, metrics),
        indexService = new TimedIndexService(indexAndWriteService.indexService, metrics),
        authorizer = authorizer,
        engine = SandboxServer.engine,
        timeProvider = timeProvider,
        timeProviderType = timeProviderType,
        ledgerConfiguration = ledgerConfiguration,
        commandConfig = config.commandConfig,
        partyConfig = PartyConfiguration.default.copy(
          // sandbox-classic always allocates party implicitly
          implicitPartyAllocation = true,
        ),
        optTimeServiceBackend = timeServiceBackendO,
        metrics = metrics,
        healthChecks = healthChecks,
        seedService = seedingService,
      )(materializer, executionSequencerFactory, logCtx)
        .map(_.withServices(List(resetService)))
      apiServer <- new LedgerApiServer(
        apiServicesOwner,
        // NOTE: Re-use the same port after reset.
        currentPort.getOrElse(config.port),
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig.flatMap(_.server),
        List(
          AuthorizationInterceptor(authService, executionContext),
          resetService,
        ),
        metrics
      ).acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port))
    } yield {
      Banner.show(Console.out)
      logger.withoutContext.info(
        "Initialized sandbox version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}, contract ids seeding = {}",
        BuildInfo.Version,
        ledgerId,
        apiServer.port.toString,
        config.damlPackages,
        timeProviderType.description,
        ledgerType,
        authService.getClass.getSimpleName,
        config.seeding.fold("no")(_.toString.toLowerCase),
      )
      if (config.scenario.nonEmpty) {
        logger.withoutContext.warn(
          """|Initializing a ledger with scenarios is deprecated and will be removed in the future. You are advised to use DAML Script instead. Using scenarios in DAML Studio will continue to work as expected.
             |A migration guide for converting your scenarios to DAML Script is available at https://docs.daml.com/daml-script/#using-daml-script-for-ledger-initialization""".stripMargin)
      }
      apiServer
    }
  }

  private def start(): Future[SandboxState] = {
    newLoggingContext(logging.participantId(participantId)) { implicit logCtx =>
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

  private def writePortFile(port: Port)(implicit executionContext: ExecutionContext): Future[Unit] =
    config.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.successful(()))
}
