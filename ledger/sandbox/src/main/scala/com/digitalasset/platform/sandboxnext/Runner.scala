// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import java.io.File
import java.time.{Clock, Instant}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.caching
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.auth.{AuthServiceWildcard, Authorizer}
import com.daml.ledger.api.domain
import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.daml.ledger.on.sql.SqlLedgerReaderWriter
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.kvutils.caching._
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v1.{SeedService, WritePackagesService}
import com.daml.lf.archive.DarReader
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.platform.apiserver._
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.PartyConfiguration
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, StandaloneIndexerServer}
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.{InvalidConfigException, SandboxConfig}
import com.daml.platform.sandbox.metrics.MetricsReporting
import com.daml.platform.sandbox.services.SandboxResetService
import com.daml.platform.sandboxnext.Runner._
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.daml.resources.akka.AkkaResourceOwner
import com.daml.resources.{ResettableResourceOwner, Resource, ResourceOwner}
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
  * Runs Sandbox with a KV SQL ledger backend.
  *
  * Known issues:
  *   - does not support implicit party allocation
  *   - does not support scenarios
  */
class Runner(config: SandboxConfig) extends ResourceOwner[Port] {
  private val specifiedLedgerId: Option[v1.LedgerId] = config.ledgerIdMode match {
    case LedgerIdMode.Static(ledgerId) =>
      Some(Ref.LedgerString.assertFromString(ledgerId.unwrap))
    case LedgerIdMode.Dynamic =>
      None
  }
  private val engine = Engine()

  private val (ledgerType, ledgerJdbcUrl, indexJdbcUrl, startupMode): (
      String,
      String,
      String,
      StartupMode) =
    config.jdbcUrl match {
      case Some(url) if url.startsWith("jdbc:postgresql:") =>
        ("PostgreSQL", url, url, StartupMode.MigrateAndStart)
      case Some(url) if url.startsWith("jdbc:h2:mem:") =>
        ("in-memory", InMemoryLedgerJdbcUrl, url, StartupMode.MigrateAndStart)
      case Some(url) if url.startsWith("jdbc:h2:") =>
        throw new InvalidDatabaseException(
          "This version of Sandbox does not support file-based H2 databases. Please use SQLite instead.")
      case Some(url) if url.startsWith("jdbc:sqlite:") =>
        ("SQLite", url, InMemoryIndexJdbcUrl, StartupMode.MigrateAndStart)
      case Some(url) =>
        throw new InvalidDatabaseException(s"Unknown database: $url")
      case None =>
        ("in-memory", InMemoryLedgerJdbcUrl, InMemoryIndexJdbcUrl, StartupMode.MigrateAndStart)
    }

  private val timeProviderType =
    config.timeProviderType.getOrElse(SandboxConfig.DefaultTimeProviderType)

  private val seeding = config.seeding.getOrElse {
    throw new InvalidConfigException(
      "This version of Sandbox will not start without a seeding mode. Please specify an appropriate seeding mode.")
  }

  if (config.scenario.isDefined) {
    throw new InvalidConfigException(
      """|This version of Sandbox does not support initialization scenarios. Please use DAML Script instead.
         |A migration guide for converting your scenarios to DAML Script is available at:
         |https://docs.daml.com/daml-script/#using-daml-script-for-ledger-initialization
         |""".stripMargin.stripLineEnd)
  }

  override def acquire()(implicit executionContext: ExecutionContext): Resource[Port] =
    newLoggingContext { implicit logCtx =>
      implicit val actorSystem: ActorSystem = ActorSystem("sandbox")
      implicit val materializer: Materializer = Materializer(actorSystem)

      val owner = for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits within a `for` comprehension.
        _ <- AkkaResourceOwner.forActorSystem(() => actorSystem)
        _ <- AkkaResourceOwner.forMaterializer(() => materializer)

        apiServer <- ResettableResourceOwner[ApiServer, (Option[Port], StartupMode)](
          initialValue = (None, startupMode),
          owner = reset => {
            case (currentPort, startupMode) =>
              for {
                metrics <- new MetricsReporting(
                  getClass.getName,
                  config.metricsReporter,
                  config.metricsReportingInterval,
                )
                timeServiceBackend = timeProviderType match {
                  case TimeProviderType.Static =>
                    Some(TimeServiceBackend.simple(Instant.EPOCH))
                  case TimeProviderType.WallClock =>
                    None
                }
                isReset = startupMode == StartupMode.ResetAndStart
                readerWriter <- new SqlLedgerReaderWriter.Owner(
                  initialLedgerId = specifiedLedgerId,
                  participantId = ParticipantId,
                  metrics = metrics,
                  jdbcUrl = ledgerJdbcUrl,
                  resetOnStartup = isReset,
                  timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
                  seedService = SeedService(seeding),
                  stateValueCache = caching.Cache.from(
                    caching.Configuration(
                      maximumWeight = MaximumStateValueCacheSize,
                    )),
                  engine = engine
                )
                ledger = new KeyValueParticipantState(readerWriter, readerWriter, metrics)
                readService = new TimedReadService(ledger, metrics)
                writeService = new TimedWriteService(ledger, metrics)
                ledgerId <- ResourceOwner.forFuture(() =>
                  readService.getLedgerInitialConditions().runWith(Sink.head).map(_.ledgerId))
                _ <- if (isReset) {
                  ResourceOwner.unit
                } else {
                  ResourceOwner
                    .forFuture(() =>
                      Future.sequence(config.damlPackages.map(uploadDar(_, writeService))))
                    .map(_ => ())
                }
                _ <- new StandaloneIndexerServer(
                  readService = readService,
                  config = IndexerConfig(
                    ParticipantId,
                    jdbcUrl = indexJdbcUrl,
                    startupMode =
                      if (isReset) IndexerStartupMode.ResetAndStart
                      else IndexerStartupMode.MigrateAndStart,
                    eventsPageSize = config.eventsPageSize,
                    allowExistingSchema = true,
                  ),
                  metrics = metrics,
                )
                authService = config.authService.getOrElse(AuthServiceWildcard)
                promise = Promise[Unit]
                resetService = {
                  val clock = Clock.systemUTC()
                  val authorizer = new Authorizer(() => clock.instant(), ledgerId, ParticipantId)
                  new SandboxResetService(
                    domain.LedgerId(ledgerId),
                    () => {
                      // Don't block the reset request; just wait until the services are closed.
                      // Otherwise we end up in deadlock, because the server won't shut down until
                      // all requests are completed.
                      reset()
                      promise.future
                    },
                    authorizer
                  )
                }
                apiServer <- new StandaloneApiServer(
                  config = ApiServerConfig(
                    participantId = ParticipantId,
                    archiveFiles = if (isReset) List.empty else config.damlPackages,
                    // Re-use the same port when resetting the server.
                    port = currentPort.getOrElse(config.port),
                    address = config.address,
                    jdbcUrl = indexJdbcUrl,
                    tlsConfig = config.tlsConfig,
                    maxInboundMessageSize = config.maxInboundMessageSize,
                    eventsPageSize = config.eventsPageSize,
                    portFile = config.portFile,
                    seeding = seeding,
                  ),
                  engine = engine,
                  commandConfig = config.commandConfig,
                  partyConfig = PartyConfiguration.default.copy(
                    implicitPartyAllocation = config.implicitPartyAllocation,
                  ),
                  ledgerConfig = config.ledgerConfig,
                  readService = readService,
                  writeService = writeService,
                  authService = authService,
                  transformIndexService = new TimedIndexService(_, metrics),
                  metrics = metrics,
                  timeServiceBackend = timeServiceBackend,
                  otherServices = List(resetService),
                  otherInterceptors = List(resetService),
                )
                _ = promise.completeWith(apiServer.servicesClosed())
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
                  seeding.toString.toLowerCase,
                )
                apiServer
              }
          },
          resetOperation =
            apiServer => Future.successful((Some(apiServer.port), StartupMode.ResetAndStart))
        )
      } yield apiServer.port

      owner.acquire()
    }

  private def uploadDar(from: File, to: WritePackagesService)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    val submissionId = v1.SubmissionId.assertFromString(UUID.randomUUID().toString)
    for {
      dar <- Future(
        DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchiveFromFile(from).get)
      _ <- to.uploadPackages(submissionId, dar.all, None).toScala
    } yield ()
  }
}

object Runner {
  private val logger = ContextualizedLogger.get(classOf[Runner])

  private val ParticipantId: v1.ParticipantId =
    Ref.ParticipantId.assertFromString("sandbox-participant")

  private val InMemoryLedgerJdbcUrl =
    "jdbc:sqlite:file:ledger?mode=memory&cache=shared"

  private val InMemoryIndexJdbcUrl =
    "jdbc:h2:mem:index;db_close_delay=-1;db_close_on_exit=false"

  private val MaximumStateValueCacheSize: caching.Cache.Size = 128L * 1024 * 1024
}
