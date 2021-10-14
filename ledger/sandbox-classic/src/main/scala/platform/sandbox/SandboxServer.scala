// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.dec.DirectExecutionContext
import com.daml.error.{ErrorCodesVersionSwitcher, ValueSwitch}
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard, Authorizer}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.participant.state.v2.metrics.TimedWriteService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.ImmArray
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.{
  LegacyTransactionCommitter,
  StandardTransactionCommitter,
  TransactionCommitter,
}
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, MetricsReporting}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver._
import com.daml.platform.configuration.{InvalidConfigException, PartyConfiguration}
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.SandboxServer._
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.SandboxConfig.EngineMode
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import com.daml.platform.sandbox.services.SandboxResetService
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger._
import com.daml.platform.sandbox.stores.ledger.sql.SqlStartMode
import com.daml.platform.sandbox.stores.{InMemoryActiveLedgerState, SandboxIndexAndWriteService}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.{FlywayMigrations, LfValueTranslationCache}
import com.daml.ports.Port
import scalaz.syntax.tag._

import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try

object SandboxServer {

  private val DefaultName = LedgerName("Sandbox")

  private val AsyncTolerance = 30.seconds

  private val logger = ContextualizedLogger.get(this.getClass)

  // We memoize the engine between resets so we avoid the expensive
  // repeated validation of the sames packages after each reset
  private[this] var engine: Option[Engine] = None

  private def getEngine(config: EngineConfig): Engine = synchronized {
    engine match {
      case Some(eng) if eng.config == config => eng
      case _ =>
        val eng = new Engine(config)
        engine = Some(eng)
        eng
    }
  }

  // Only used for testing.
  def owner(config: SandboxConfig): ResourceOwner[SandboxServer] =
    owner(DefaultName, config)

  def owner(name: LedgerName, config: SandboxConfig): ResourceOwner[SandboxServer] =
    for {
      metrics <- new MetricsReporting(
        classOf[SandboxServer].getName,
        config.metricsReporter,
        config.metricsReportingInterval,
      )
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(name.unwrap.toLowerCase()))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- ResourceOwner
        .forTryCloseable(() => Try(new SandboxServer(name, config, materializer, metrics)))
      // Wait for the API server to start.
      _ <- new ResourceOwner[Unit] {
        override def acquire()(implicit context: ResourceContext): Resource[Unit] =
          // We use the Future rather than the Resource to avoid holding onto the API server.
          // Otherwise, we cause a memory leak upon reset.
          Resource.fromFuture(server.apiServer.map(_ => ()))
      }
    } yield server

  // Run only the flyway migrations but do not initialize any of the ledger api or indexer services
  def migrateOnly(
      config: SandboxConfig
  )(implicit resourceContext: ResourceContext): Future[Unit] = {

    newLoggingContextWith(logging.participantId(config.participantId)) { implicit loggingContext =>
      logger.info("Running only schema migration scripts")
      new FlywayMigrations(config.jdbcUrl.get, config.enableAppendOnlySchema)
        .migrate()
    }
  }

  final class SandboxState(
      materializer: Materializer,
      metrics: Metrics,
      packageStore: InMemoryPackageStore,
      // nested resource so we can release it independently when restarting
      apiServerResource: Resource[ApiServer],
  ) {
    def port(implicit executionContext: ExecutionContext): Future[Port] =
      apiServer.map(_.port)

    private[SandboxServer] def apiServer: Future[ApiServer] =
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

    def release(): Future[Unit] =
      apiServerResource.release()
  }

}

final class SandboxServer(
    name: LedgerName,
    config: SandboxConfig,
    materializer: Materializer,
    metrics: Metrics,
) extends AutoCloseable {

  private[this] val engine = {
    val engineConfig = {
      val allowedLanguageVersions =
        config.engineMode match {
          case EngineMode.Stable if config.seeding.nonEmpty =>
            LanguageVersion.StableVersions
          case EngineMode.Stable =>
            LanguageVersion.LegacyVersions
          case EngineMode.EarlyAccess if config.seeding.nonEmpty =>
            LanguageVersion.EarlyAccessVersions
          case EngineMode.Dev if config.seeding.nonEmpty =>
            LanguageVersion.DevVersions
          case mode =>
            throw new InvalidConfigException(
              s""""${Seeding.NoSeedingModeName}" contract IDs seeding mode is not compatible with $mode mode"""
            )
        }
      EngineConfig(
        allowedLanguageVersions = allowedLanguageVersions,
        profileDir = config.profileDir,
        stackTraceMode = config.stackTraces,
        forbidV0ContractId = false,
      )
    }
    getEngine(engineConfig)
  }

  // Only used for testing.
  def this(config: SandboxConfig, materializer: Materializer) =
    this(DefaultName, config, materializer, new Metrics(new MetricRegistry))

  private val authService: AuthService = config.authService.getOrElse(AuthServiceWildcard)
  private val seedingService = SeedService(config.seeding.getOrElse(Seeding.Weak))

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

  def resetAndRestartServer()(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Unit] = {
    val apiServicesClosed = apiServer.flatMap(_.servicesClosed())

    // TODO: eliminate the state mutation somehow
    sandboxState = sandboxState.flatMap(
      _.reset((materializer, metrics, packageStore, port) =>
        buildAndStartApiServer(
          materializer = materializer,
          metrics = metrics,
          packageStore = packageStore,
          startMode = SqlStartMode.ResetAndStart,
          currentPort = Some(port),
        )
      )
    )

    // Wait for the services to be closed, so we can guarantee that future API calls after finishing
    // the reset will never be handled by the old one.
    apiServicesClosed
  }

  // if requested, initialize the ledger state with the given scenario
  private def createInitialState(
      config: SandboxConfig,
      packageStore: InMemoryPackageStore,
  ): (InMemoryActiveLedgerState, ImmArray[LedgerEntryOrBump], Option[Instant]) = {
    // [[ScenarioLoader]] needs all the packages to be already compiled --
    // make sure that that's the case
    if (config.eagerPackageLoading || config.scenario.nonEmpty) {
      for (pkgId <- packageStore.listLfPackagesSync().keys) {
        val pkg = packageStore.getLfPackageSync(pkgId).get
        engine
          .preloadPackage(pkgId, pkg)
          .consume(
            pcs = { _ =>
              sys.error("Unexpected request of contract")
            },
            packages = packageStore.getLfPackageSync,
            keys = { _ =>
              sys.error("Unexpected request of contract key")
            },
          )
          .left
          .foreach(err => sys.error(err.message))
      }
    }
    config.scenario match {
      case None => (InMemoryActiveLedgerState.empty, ImmArray.Empty, None)
      case Some(scenario) =>
        val (acs, records, ledgerTime) =
          ScenarioLoader.fromScenario(
            packageStore,
            engine,
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
  )(implicit loggingContext: LoggingContext): Resource[ApiServer] = {
    implicit val _materializer: Materializer = materializer
    implicit val actorSystem: ActorSystem = materializer.system
    implicit val executionContext: ExecutionContext = materializer.executionContext
    implicit val resourceContext: ResourceContext = ResourceContext(executionContext)

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

    val lfValueTranslationCache =
      LfValueTranslationCache.Cache.newInstrumentedInstance(
        eventConfiguration = config.lfValueTranslationEventCacheConfiguration,
        contractConfiguration = config.lfValueTranslationContractCacheConfiguration,
        metrics = metrics,
      )

    val ledgerType = config.jdbcUrl match {
      case Some(_) => "postgres"
      case None => "in-memory"
    }

    for {
      servicesExecutionContext <- ResourceOwner
        .forExecutorService(() => Executors.newWorkStealingPool())
        .map(ExecutionContext.fromExecutorService)
        .acquire()
      indexAndWriteService <- (config.jdbcUrl match {
        case Some(jdbcUrl) =>
          SandboxIndexAndWriteService.postgres(
            name = name,
            providedLedgerId = config.ledgerIdMode,
            participantId = config.participantId,
            jdbcUrl = jdbcUrl,
            databaseConnectionPoolSize = config.databaseConnectionPoolSize,
            databaseConnectionTimeout = config.databaseConnectionTimeout,
            timeProvider = timeProvider,
            ledgerEntries = ledgerEntries,
            startMode = startMode,
            queueDepth = config.maxParallelSubmissions,
            transactionCommitter = transactionCommitter,
            templateStore = packageStore,
            eventsPageSize = config.eventsPageSize,
            eventsProcessingParallelism = config.eventsProcessingParallelism,
            servicesExecutionContext = servicesExecutionContext,
            metrics = metrics,
            lfValueTranslationCache = lfValueTranslationCache,
            engine = engine,
            validatePartyAllocation = !config.implicitPartyAllocation,
            enableAppendOnlySchema = config.enableAppendOnlySchema,
            enableCompression = config.enableCompression,
          )
        case None =>
          SandboxIndexAndWriteService.inMemory(
            name,
            config.ledgerIdMode,
            config.participantId,
            timeProvider,
            acs,
            ledgerEntries,
            transactionCommitter,
            packageStore,
            metrics,
            engine,
          )
      }).acquire()
      ledgerId <- Resource.fromFuture(indexAndWriteService.indexService.getLedgerId())
      authorizer = new Authorizer(
        () => java.time.Clock.systemUTC.instant(),
        LedgerId.unwrap(ledgerId),
        config.participantId,
        new ErrorCodesVersionSwitcher(config.enableSelfServiceErrorCodes),
      )
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
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner().acquire()
      apiServicesOwner = new ApiServices.Owner(
        participantId = config.participantId,
        optWriteService = Some(new TimedWriteService(indexAndWriteService.writeService, metrics)),
        indexService = new TimedIndexService(indexAndWriteService.indexService, metrics),
        authorizer = authorizer,
        engine = engine,
        timeProvider = timeProvider,
        timeProviderType = timeProviderType,
        configurationLoadTimeout = config.configurationLoadTimeout,
        initialLedgerConfiguration = Some(config.initialLedgerConfiguration),
        commandConfig = config.commandConfig,
        partyConfig = PartyConfiguration.default.copy(
          implicitPartyAllocation = config.implicitPartyAllocation
        ),
        submissionConfig = config.submissionConfig,
        optTimeServiceBackend = timeServiceBackendO,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        healthChecks = healthChecks,
        seedService = seedingService,
        managementServiceTimeout = config.managementServiceTimeout,
        enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
      )(materializer, executionSequencerFactory, loggingContext)
        .map(_.withServices(List(resetService)))
      apiServer <- new LedgerApiServer(
        apiServicesOwner,
        // NOTE: Re-use the same port after reset.
        currentPort.getOrElse(config.port),
        config.maxInboundMessageSize,
        config.address,
        config.tlsConfig,
        List(
          AuthorizationInterceptor(
            authService,
            executionContext,
            new ValueSwitch(config.enableSelfServiceErrorCodes),
          ),
          resetService,
        ),
        servicesExecutionContext,
        metrics,
      ).acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port))
    } yield {
      Banner.show(Console.out)
      logger.withoutContext.info(
        s"Initialized {} version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}, contract ids seeding = {}{}{}",
        LedgerName.unwrap(name),
        BuildInfo.Version,
        LedgerId.unwrap(ledgerId),
        apiServer.port.toString,
        config.damlPackages,
        timeProviderType.description,
        ledgerType,
        authService.getClass.getSimpleName,
        config.seeding.fold(Seeding.NoSeedingModeName)(_.name),
        if (config.stackTraces) "" else ", stack traces = no",
        config.profileDir match {
          case None => ""
          case Some(profileDir) => s", profile directory = $profileDir"
        },
      )
      if (config.scenario.nonEmpty) {
        logger.withoutContext.warn(
          """|Initializing a ledger with scenarios is deprecated and will be removed in the future. You are advised to use Daml Script instead. Using scenarios in Daml Studio will continue to work as expected.
             |A migration guide for converting your scenarios to Daml Script is available at https://docs.daml.com/daml-script/#using-daml-script-for-ledger-initialization""".stripMargin
        )
      }
      if (config.engineMode == SandboxConfig.EngineMode.EarlyAccess) {
        logger.withoutContext.warn(
          """|Using early access mode is dangerous as the backward compatibility of future SDKs is not guaranteed.
             |Should be used for testing purpose only.""".stripMargin
        )
      }
      if (config.seeding.isEmpty) {
        logger.withoutContext.warn(
          s"""|'${Seeding.NoSeedingModeName}' contract IDs seeding mode is not compatible with the LF 1.11 languages or later.
              |A ledger stared with ${Seeding.NoSeedingModeName} contract IDs seeding will refuse to load LF 1.11 language or later.
              |To make sure you can load LF 1.11, use the option '--contract-id-seeding=strong' to set up the contract IDs seeding mode.""".stripMargin
        )
      }
      apiServer
    }
  }

  private def start(): Future[SandboxState] = {
    newLoggingContextWith(logging.participantId(config.participantId)) { implicit loggingContext =>
      val packageStore = loadDamlPackages()
      val apiServerResource = buildAndStartApiServer(
        materializer,
        metrics,
        packageStore,
        SqlStartMode
          .fromString(config.sqlStartMode.toString)
          .getOrElse(SqlStartMode.MigrateAndStart),
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
      .getOrElse(Future.unit)
}
