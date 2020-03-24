// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import java.io.File
import java.time.{Clock, Instant}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.daml.ledger.on.sql.SqlLedgerReaderWriter
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{SeedService, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.buildinfo.BuildInfo
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthServiceWildcard, Authorizer}
import com.digitalasset.ledger.api.domain
import com.digitalasset.logging.ContextualizedLogger
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.apiserver.{
  ApiServer,
  ApiServerConfig,
  StandaloneApiServer,
  TimeServiceBackend
}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.indexer.{
  IndexerConfig,
  IndexerStartupMode,
  StandaloneIndexerServer
}
import com.digitalasset.platform.sandbox.banner.Banner
import com.digitalasset.platform.sandbox.config.{InvalidConfigException, SandboxConfig}
import com.digitalasset.platform.sandbox.metrics.MetricsReporting
import com.digitalasset.platform.sandbox.services.SandboxResetService
import com.digitalasset.platform.sandboxnext.Runner._
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.platform.state.{TimedIndexService, TimedReadService, TimedWriteService}
import com.digitalasset.platform.store.FlywayMigrations
import com.digitalasset.ports.Port
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{ResettableResourceOwner, Resource, ResourceOwner}
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.duration.{DurationInt, FiniteDuration}
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

  private val (ledgerType, ledgerJdbcUrl, indexJdbcUrl, startupMode): (
      String,
      String,
      String,
      StartupMode) =
    config.jdbcUrl match {
      case Some(url) if url.startsWith("jdbc:postgresql:") =>
        ("PostgreSQL", url, url, StartupMode.MigrateAndStart)
      case Some(url) if url.startsWith("jdbc:h2:mem:") =>
        ("in-memory", InMemoryLedgerJdbcUrl, url, StartupMode.ResetAndStart)
      case Some(url) if url.startsWith("jdbc:h2:") =>
        throw new InvalidDatabaseException(
          "This version of Sandbox does not support file-based H2 databases. Please use SQLite instead.")
      case Some(url) if url.startsWith("jdbc:sqlite:") =>
        ("SQLite", url, InMemoryIndexJdbcUrl, StartupMode.MigrateAndStart)
      case Some(url) =>
        throw new InvalidDatabaseException(s"Unknown database: $url")
      case None =>
        ("in-memory", InMemoryLedgerJdbcUrl, InMemoryIndexJdbcUrl, StartupMode.ResetAndStart)
    }

  private val timeProviderType = config.timeProviderType.getOrElse {
    throw new InvalidConfigException(
      "Sandbox used to default to Static Time mode. In the next release, Wall Clock Time mode will"
        + " become the default. In this version, you will need to explicitly specify the"
        + " `--static-time` flag to maintain the previous behavior, or `--wall-clock-time` if you"
        + " would like to use the new defaults.")
  }

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

      val (timeServiceBackend, heartbeatMechanism) = timeProviderType match {
        case TimeProviderType.Static =>
          val backend = TimeServiceBackend.observing(TimeServiceBackend.simple(Instant.EPOCH))
          (Some(backend), backend.changes)
        case TimeProviderType.WallClock =>
          val clock = Clock.systemUTC()
          (None, new RegularHeartbeat(clock, HeartbeatInterval))
      }

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
                _ <- startupMode match {
                  case StartupMode.MigrateAndStart =>
                    ResourceOwner.successful(())
                  case StartupMode.ResetAndStart =>
                    // Resetting through Flyway removes all tables in the database schema.
                    // Therefore we don't need to "reset" the KV Ledger and Index separately.
                    ResourceOwner.forFuture(() => new FlywayMigrations(indexJdbcUrl).reset())
                }
                heartbeats <- heartbeatMechanism
                readerWriter <- new SqlLedgerReaderWriter.Owner(
                  initialLedgerId = specifiedLedgerId,
                  participantId = ParticipantId,
                  jdbcUrl = ledgerJdbcUrl,
                  timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
                  heartbeats = heartbeats,
                  seedService = SeedService(seeding)
                )
                ledger = new KeyValueParticipantState(readerWriter, readerWriter)
                readService = new TimedReadService(ledger, metrics, ReadServicePrefix)
                writeService = new TimedWriteService(ledger, metrics, WriteServicePrefix)
                ledgerId <- ResourceOwner.forFuture(() =>
                  readService.getLedgerInitialConditions().runWith(Sink.head).map(_.ledgerId))
                _ <- ResourceOwner.forFuture(() =>
                  Future.sequence(config.damlPackages.map(uploadDar(_, writeService))))
                _ <- new StandaloneIndexerServer(
                  readService = readService,
                  config = IndexerConfig(
                    ParticipantId,
                    jdbcUrl = indexJdbcUrl,
                    startupMode = IndexerStartupMode.MigrateAndStart,
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
                    archiveFiles = config.damlPackages,
                    // Re-use the same port when resetting the server.
                    port = currentPort.getOrElse(config.port),
                    address = config.address,
                    jdbcUrl = indexJdbcUrl,
                    tlsConfig = config.tlsConfig,
                    maxInboundMessageSize = config.maxInboundMessageSize,
                    portFile = config.portFile,
                  ),
                  commandConfig = config.commandConfig,
                  partyConfig = config.partyConfig,
                  submissionConfig = config.submissionConfig,
                  readService = readService,
                  writeService = writeService,
                  authService = authService,
                  transformIndexService = new TimedIndexService(_, metrics, IndexServicePrefix),
                  metrics = metrics,
                  timeServiceBackend = timeServiceBackend,
                  seeding = Some(seeding),
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

  private def uploadDar(from: File, to: WriteService)(
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

  private val ReadServicePrefix = "daml.services.read"
  private val IndexServicePrefix = "daml.services.index"
  private val WriteServicePrefix = "daml.services.write"

  private val HeartbeatInterval: FiniteDuration = 1.second
}
