// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import java.io.File
import java.time.{Clock, Instant}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.daml.ledger.on.sql.SqlLedgerReaderWriter
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthServiceWildcard, Authorizer}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.apiserver.{
  ApiServer,
  ApiServerConfig,
  StandaloneApiServer,
  TimeServiceBackend
}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.configuration.BuildInfo
import com.digitalasset.platform.indexer.{
  IndexerConfig,
  IndexerStartupMode,
  StandaloneIndexerServer
}
import com.digitalasset.platform.sandbox.banner.Banner
import com.digitalasset.platform.sandbox.config.{InvalidConfigException, SandboxConfig}
import com.digitalasset.platform.sandbox.services.SandboxResetService
import com.digitalasset.platform.sandboxnext.Runner._
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.ports.Port
import com.digitalasset.resources.ResettableResourceOwner.Reset
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{ResettableResourceOwner, Resource, ResourceOwner}
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Runs Sandbox with a KV SQL ledger backend.
  *
  * Known issues:
  *   - does not support implicit party allocation
  *   - does not support scenarios
  *   - does not provide the reset service
  */
class Runner(config: SandboxConfig) extends ResourceOwner[Port] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[Port] = {
    implicit val system: ActorSystem = ActorSystem("sandbox")
    implicit val materializer: Materializer = Materializer(system)

    val specifiedLedgerId: Option[v1.LedgerId] = config.ledgerIdMode match {
      case LedgerIdMode.Static(ledgerId) =>
        Some(Ref.LedgerString.assertFromString(ledgerId.unwrap))
      case LedgerIdMode.Dynamic =>
        None
    }

    val (ledgerType, ledgerJdbcUrl, indexJdbcUrl, indexerStartupMode): (
        String,
        String,
        String,
        IndexerStartupMode) = config.jdbcUrl match {
      case Some(url) if url.startsWith("jdbc:postgresql:") =>
        ("PostgreSQL", url, url, IndexerStartupMode.MigrateAndStart)
      case Some(url) if url.startsWith("jdbc:h2:mem:") =>
        ("in-memory", InMemoryLedgerJdbcUrl, url, IndexerStartupMode.ResetAndStart)
      case Some(url) if url.startsWith("jdbc:h2:") =>
        throw new InvalidDatabaseException(
          "This version of Sandbox does not support file-based H2 databases. Please use SQLite instead.")
      case Some(url) if url.startsWith("jdbc:sqlite:") =>
        ("SQLite", url, InMemoryIndexJdbcUrl, IndexerStartupMode.ResetAndStart)
      case Some(url) =>
        throw new InvalidDatabaseException(s"Unknown database: $url")
      case None =>
        ("in-memory", InMemoryLedgerJdbcUrl, InMemoryIndexJdbcUrl, IndexerStartupMode.ResetAndStart)
    }

    val timeProviderType = config.timeProviderType.getOrElse(TimeProviderType.Static)
    val (timeServiceBackend, heartbeatMechanism) = timeProviderType match {
      case TimeProviderType.Static =>
        val backend = TimeServiceBackend.observing(TimeServiceBackend.simple(Instant.EPOCH))
        (Some(backend), backend.changes)
      case TimeProviderType.WallClock =>
        val clock = Clock.systemUTC()
        (None, new RegularHeartbeat(clock, HeartbeatInterval))
    }

    val seeding = config.seeding.getOrElse {
      throw new InvalidConfigException(
        "This version of Sandbox will not start without a seeding mode. Please specify an appropriate seeding mode.")
    }

    val owner = newLoggingContext { implicit logCtx =>
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits within a `for` comprehension.
        _ <- AkkaResourceOwner.forActorSystem(() => system)
        _ <- AkkaResourceOwner.forMaterializer(() => materializer)

        apiServer <- ResettableResourceOwner[ApiServer, (Option[Port], IndexerStartupMode)](
          initialValue = (None, indexerStartupMode),
          owner = reset => {
            case (currentPort, startupMode) =>
              for {
                heartbeats <- heartbeatMechanism
                readerWriter <- SqlLedgerReaderWriter.owner(
                  initialLedgerId = specifiedLedgerId,
                  participantId = ParticipantId,
                  jdbcUrl = ledgerJdbcUrl,
                  timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
                  heartbeats = heartbeats,
                )
                ledger = new KeyValueParticipantState(readerWriter, readerWriter)
                ledgerId <- ResourceOwner.forFuture(() =>
                  ledger.getLedgerInitialConditions().runWith(Sink.head).map(_.ledgerId))
                authService = config.authService.getOrElse(AuthServiceWildcard)
                _ <- ResourceOwner.forFuture(() =>
                  Future.sequence(config.damlPackages.map(uploadDar(_, ledger))))
                domainLedgerId = LedgerId(ledgerId)
                resetService <- new SandboxResetServiceOwner(domainLedgerId, reset)
                _ <- new StandaloneIndexerServer(
                  readService = ledger,
                  config = IndexerConfig(
                    ParticipantId,
                    jdbcUrl = indexJdbcUrl,
                    startupMode = startupMode,
                    allowExistingSchema = true,
                  ),
                  metrics = SharedMetricRegistries.getOrCreate(s"indexer-$ParticipantId"),
                )
                apiServer <- new StandaloneApiServer(
                  ApiServerConfig(
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
                  submissionConfig = config.submissionConfig,
                  readService = ledger,
                  writeService = ledger,
                  authService = authService,
                  metrics = SharedMetricRegistries.getOrCreate(s"ledger-api-server-$ParticipantId"),
                  timeServiceBackend = timeServiceBackend,
                  seeding = seeding,
                  otherServices = List(resetService),
                  otherInterceptors = List(resetService),
                )
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
          resetOperation = apiServer =>
            apiServer
              .servicesClosed()
              .map(_ => (Some(apiServer.port), IndexerStartupMode.ResetAndStart)),
        )
      } yield apiServer.port
    }

    owner.acquire()
  }

  private def uploadDar(from: File, to: KeyValueParticipantState)(
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

  private val HeartbeatInterval: FiniteDuration = 1.second

  private final class SandboxResetServiceOwner(ledgerId: LedgerId, reset: Reset)(
      implicit logCtx: LoggingContext
  ) extends ResourceOwner[SandboxResetService] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[SandboxResetService] = {
      val clock = Clock.systemUTC()
      Resource.successful(
        new SandboxResetService(
          ledgerId,
          reset,
          new Authorizer(() => clock.instant(), LedgerId.unwrap(ledgerId), ParticipantId),
        ))
    }
  }

}
