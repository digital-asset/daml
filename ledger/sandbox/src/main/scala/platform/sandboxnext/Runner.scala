// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.caching
import com.daml.ledger.api.auth.AuthServiceWildcard
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalContractIds,
}
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.daml.ledger.on.sql.SqlLedgerReaderWriter
import com.daml.ledger.participant.state.kvutils.api.{
  KeyValueParticipantStateReader,
  KeyValueParticipantStateWriter,
  TimedLedgerWriter,
  WriteServiceWithDeduplicationSupport,
}
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.ledger.participant.state.v2.WritePackagesService
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.LanguageVersion
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.MetricsReporting
import com.daml.platform.apiserver._
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.{PartyConfiguration, ServerRole, SubmissionConfiguration}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, StandaloneIndexerServer}
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.config.SandboxConfig.EngineMode
import com.daml.platform.sandboxnext.Runner._
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.platform.usermanagement.PersistentUserManagementStore
import com.daml.ports.Port
import com.daml.telemetry.{DefaultTelemetry, SpanKind, SpanName}
import scalaz.syntax.tag._

import scala.jdk.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Runs Sandbox with a KV SQL ledger backend.
  *
  * Known issues:
  *   - does not support implicit party allocation
  *   - does not support scenarios
  */
class Runner(config: SandboxConfig) extends ResourceOwner[Port] {
  private val specifiedLedgerId: Option[LedgerId] = config.ledgerIdMode match {
    case LedgerIdMode.Static(ledgerId) =>
      Some(Ref.LedgerString.assertFromString(ledgerId.unwrap))
    case LedgerIdMode.Dynamic =>
      None
  }

  private[this] val engine = {
    val languageVersions =
      config.engineMode match {
        case EngineMode.Dev => LanguageVersion.DevVersions
        case EngineMode.EarlyAccess => LanguageVersion.EarlyAccessVersions
        case EngineMode.Stable => LanguageVersion.StableVersions
      }
    val engineConfig = EngineConfig(
      allowedLanguageVersions = languageVersions,
      profileDir = config.profileDir,
      stackTraceMode = config.stackTraces,
      forbidV0ContractId = true,
    )
    new Engine(engineConfig)
  }

  private val (ledgerType, ledgerJdbcUrl, indexJdbcUrl): (
      String,
      String,
      String,
  ) =
    config.jdbcUrl match {
      case Some(url) if url.startsWith("jdbc:postgresql:") =>
        ("PostgreSQL", url, url)
      case Some(url) if url.startsWith("jdbc:h2:mem:") =>
        ("in-memory", InMemoryLedgerJdbcUrl, url)
      case Some(url) if url.startsWith("jdbc:h2:") =>
        throw new InvalidDatabaseException(
          "This version of Sandbox does not support file-based H2 databases. Please use SQLite instead."
        )
      case Some(url) if url.startsWith("jdbc:sqlite:") =>
        ("SQLite", url, InMemoryIndexJdbcUrl)
      case Some(_) =>
        throw new InvalidDatabaseException(s"Unknown database")
      case None =>
        ("in-memory", InMemoryLedgerJdbcUrl, InMemoryIndexJdbcUrl)
    }

  private val timeProviderType =
    config.timeProviderType.getOrElse(SandboxConfig.DefaultTimeProviderType)

  override def acquire()(implicit context: ResourceContext): Resource[Port] =
    newLoggingContext { implicit loggingContext =>
      implicit val actorSystem: ActorSystem = ActorSystem("sandbox")
      implicit val materializer: Materializer = Materializer(actorSystem)

      val owner = for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits within a `for` comprehension.
        _ <- ResourceOwner.forActorSystem(() => actorSystem)
        _ <- ResourceOwner.forMaterializer(() => materializer)

        metrics <- new MetricsReporting(
          getClass.getName,
          config.metricsReporter,
          config.metricsReportingInterval,
        )
        lfValueTranslationCache = LfValueTranslationCache.Cache.newInstrumentedInstance(
          eventConfiguration = config.lfValueTranslationEventCacheConfiguration,
          contractConfiguration = config.lfValueTranslationContractCacheConfiguration,
          metrics = metrics,
        )

        authService = config.authService.getOrElse(AuthServiceWildcard)
        servicesExecutionContext <- ResourceOwner.forExecutorService(() =>
          ExecutionContext.fromExecutorService(Executors.newWorkStealingPool())
        )
        ledgerId = specifiedLedgerId.getOrElse(UUID.randomUUID().toString)
        timeServiceBackend = timeProviderType match {
          case TimeProviderType.Static =>
            Some(TimeServiceBackend.simple(Instant.EPOCH))
          case TimeProviderType.WallClock =>
            None
        }
        readerWriter <- new SqlLedgerReaderWriter.Owner(
          ledgerId = ledgerId,
          participantId = config.participantId,
          metrics = metrics,
          engine = engine,
          jdbcUrl = ledgerJdbcUrl,
          resetOnStartup = false,
          offsetVersion = 0,
          logEntryIdAllocator = new SeedServiceLogEntryIdAllocator(SeedService(config.seeding)),
          stateValueCache = caching.WeightedCache.from(
            caching.WeightedCache.Configuration(
              maximumWeight = MaximumStateValueCacheSize
            )
          ),
          timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        )
        readService = new TimedReadService(
          KeyValueParticipantStateReader(
            readerWriter,
            metrics,
            enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
          ),
          metrics,
        )
        writeService = new TimedWriteService(
          new KeyValueParticipantStateWriter(
            new TimedLedgerWriter(readerWriter, metrics),
            metrics,
          ),
          metrics,
        )
        _ <- ResourceOwner
          .forFuture(() => Future.sequence(config.damlPackages.map(uploadDar(_, writeService))))
          .map(_ => ())

        indexer <- new StandaloneIndexerServer(
          readService = readService,
          config = IndexerConfig(
            participantId = config.participantId,
            jdbcUrl = indexJdbcUrl,
            startupMode = IndexerStartupMode.MigrateAndStart,
            eventsPageSize = config.eventsPageSize,
            allowExistingSchema = true,
            enableCompression = config.enableCompression,
          ),
          metrics = metrics,
          lfValueTranslationCache = lfValueTranslationCache,
        )
        healthChecks = new HealthChecks(
          "read" -> readService,
          "write" -> writeService,
          "indexer" -> indexer,
        )
        // Required to tie the loop between the API server and the reset service.
        apiServerServicesClosed = Promise[Unit]()
        apiServerConfig = ApiServerConfig(
          participantId = config.participantId,
          archiveFiles = config.damlPackages,
          // Re-use the same port when resetting the server.
          port = config.port,
          address = config.address,
          jdbcUrl = indexJdbcUrl,
          databaseConnectionPoolSize = config.databaseConnectionPoolSize,
          databaseConnectionTimeout = config.databaseConnectionTimeout,
          tlsConfig = config.tlsConfig,
          maxInboundMessageSize = config.maxInboundMessageSize,
          initialLedgerConfiguration = Some(config.initialLedgerConfiguration),
          configurationLoadTimeout = config.configurationLoadTimeout,
          eventsPageSize = config.eventsPageSize,
          portFile = config.portFile,
          // TODO append-only: augment the following defaults for enabling the features for sandbox next
          seeding = config.seeding,
          managementServiceTimeout = config.managementServiceTimeout,
          maxContractStateCacheSize = 0L,
          maxContractKeyStateCacheSize = 0L,
          maxTransactionsInMemoryFanOutBufferSize = 0L,
          enableInMemoryFanOutForLedgerApi = false,
          enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
          enableUserManagement = config.userManagementConfig.enabled,
        )
        dbSupport <- DbSupport.owner(
          jdbcUrl = apiServerConfig.jdbcUrl,
          serverRole = ServerRole.ApiServer,
          connectionPoolSize = apiServerConfig.databaseConnectionPoolSize,
          connectionTimeout = apiServerConfig.databaseConnectionTimeout,
          metrics = metrics,
        )
        userManagementStore = PersistentUserManagementStore.cached(
          dbSupport = dbSupport,
          metrics = metrics,
          cacheExpiryAfterWriteInSeconds =
            config.userManagementConfig.cacheExpiryAfterWriteInSeconds,
          maximumCacheSize = config.userManagementConfig.maximumCacheSize,
        )(servicesExecutionContext)
        indexService <- StandaloneIndexService(
          dbSupport = dbSupport,
          ledgerId = ledgerId,
          config = apiServerConfig,
          metrics = metrics,
          engine = engine,
          servicesExecutionContext = servicesExecutionContext,
          lfValueTranslationCache = lfValueTranslationCache,
        )
        writeServiceWithDeduplicationSupport = WriteServiceWithDeduplicationSupport(
          writeService,
          indexService,
          config.enableSelfServiceErrorCodes,
        )
        apiServer <- StandaloneApiServer(
          indexService = indexService,
          userManagementStore = userManagementStore,
          ledgerId = ledgerId,
          config = apiServerConfig,
          engine = engine,
          commandConfig = config.commandConfig,
          partyConfig = PartyConfiguration.default.copy(
            implicitPartyAllocation = config.implicitPartyAllocation
          ),
          submissionConfig = SubmissionConfiguration.default,
          optWriteService = Some(writeServiceWithDeduplicationSupport),
          authService = authService,
          healthChecks = healthChecks,
          metrics = metrics,
          timeServiceBackend = timeServiceBackend,
          servicesExecutionContext = servicesExecutionContext,
          ledgerFeatures = LedgerFeatures(
            staticTime = timeServiceBackend.isDefined,
            commandDeduplicationFeatures = CommandDeduplicationFeatures.of(
              deduplicationPeriodSupport = Some(
                CommandDeduplicationPeriodSupport.of(
                  offsetSupport =
                    CommandDeduplicationPeriodSupport.OffsetSupport.OFFSET_CONVERT_TO_DURATION,
                  durationSupport =
                    CommandDeduplicationPeriodSupport.DurationSupport.DURATION_NATIVE_SUPPORT,
                )
              ),
              deduplicationType = CommandDeduplicationType.ASYNC_ONLY,
              maxDeduplicationDurationEnforced = true,
            ),
            contractIdFeatures = ExperimentalContractIds.of(
              v1 = ExperimentalContractIds.ContractIdV1Support.NON_SUFFIXED
            ),
          ),
        )
        _ = apiServerServicesClosed.completeWith(apiServer.servicesClosed())
      } yield {
        Banner.show(Console.out)
        logger.withoutContext.info(
          "Initialized sandbox version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}, contract ids seeding = {}{}{}",
          BuildInfo.Version,
          ledgerId,
          apiServer.port.toString,
          config.damlPackages,
          timeProviderType.description,
          ledgerType,
          authService.getClass.getSimpleName,
          config.seeding.name,
          if (config.stackTraces) "" else ", stack traces = no",
          config.profileDir match {
            case None => ""
            case Some(profileDir) => s", profile directory = $profileDir"
          },
        )
        apiServer.port
      }
      owner.acquire()
    }

  private def uploadDar(from: File, to: WritePackagesService)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Unit] = DefaultTelemetry.runFutureInSpan(SpanName.RunnerUploadDar, SpanKind.Internal) {
    implicit telemetryContext =>
      val submissionId = Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)
      for {
        dar <- Future.fromTry(DarParser.readArchiveFromFile(from).toTry)
        _ <- to.uploadPackages(submissionId, dar.all, None).asScala
      } yield ()
  }
}

object Runner {
  private val logger = ContextualizedLogger.get(classOf[Runner])

  private val InMemoryLedgerJdbcUrl =
    "jdbc:sqlite:file:ledger?mode=memory&cache=shared"

  private val InMemoryIndexJdbcUrl =
    "jdbc:h2:mem:index;db_close_delay=-1;db_close_on_exit=false"

  private val MaximumStateValueCacheSize: caching.Cache.Size = 128L * 1024 * 1024
}
