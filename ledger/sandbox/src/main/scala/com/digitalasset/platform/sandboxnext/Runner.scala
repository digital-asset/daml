// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import java.io.File
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.on.sql.Database.InvalidDatabaseException
import com.daml.ledger.on.sql.SqlLedgerReaderWriter
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{ReadService, SeedService, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.apiserver.{
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
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandboxnext.Runner._
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.resources.akka.AkkaResourceOwner
import scalaz.syntax.tag._

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Runs Sandbox with a KV SQL ledger backend.
  *
  * Known issues:
  *   - does not support implicit party allocation
  *   - does not support scenarios
  *   - does not emit heartbeats
  *   - does not provide the reset service
  */
class Runner {
  def owner(config: SandboxConfig): ResourceOwner[Unit] = {
    implicit val system: ActorSystem = ActorSystem("sandbox")
    implicit val materializer: Materializer = Materializer(system)
    implicit val executionContext: ExecutionContext = system.dispatcher

    val specifiedLedgerId: Option[v1.LedgerId] = config.ledgerIdMode match {
      case LedgerIdMode.Static(ledgerId) =>
        Some(Ref.LedgerString.assertFromString(ledgerId.unwrap))
      case LedgerIdMode.Dynamic =>
        None
    }

    val (ledgerType, ledgerJdbcUrl, indexJdbcUrl) = config.jdbcUrl match {
      case Some(url) if url.startsWith("jdbc:postgresql") => ("PostgreSQL", url, url)
      case Some(url) if url.startsWith("jdbc:h2:mem:") => ("in-memory", InMemoryLedgerJdbcUrl, url)
      case Some(url) if url.startsWith("jdbc:h2:") =>
        throw new InvalidDatabaseException(
          "This version of Sandbox does not support file-based H2 databases. Please use SQLite instead.")
      case Some(url) if url.startsWith("jdbc:sqlite:") => ("SQLite", url, InMemoryIndexJdbcUrl)
      case Some(url) => throw new InvalidDatabaseException(s"Unknown database: $url")
      case None => ("in-memory", InMemoryLedgerJdbcUrl, InMemoryIndexJdbcUrl)
    }

    val timeProviderType = config.timeProviderType.getOrElse(TimeProviderType.Static)
    val timeServiceBackend = timeProviderType match {
      case TimeProviderType.Static =>
        Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock =>
        None
    }

    newLoggingContext { implicit logCtx =>
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits within a `for` comprehension.
        _ <- AkkaResourceOwner.forActorSystem(() => system)
        _ <- AkkaResourceOwner.forMaterializer(() => materializer)
        readerWriter <- SqlLedgerReaderWriter.owner(
          initialLedgerId = specifiedLedgerId,
          participantId = ParticipantId,
          jdbcUrl = ledgerJdbcUrl,
          timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        )
        ledger = new KeyValueParticipantState(readerWriter, readerWriter)
        ledgerId <- ResourceOwner.forFuture(() =>
          ledger.getLedgerInitialConditions().runWith(Sink.head).map(_.ledgerId))
        authService = config.authService.getOrElse(AuthServiceWildcard)
        _ <- ResourceOwner.forFuture(() =>
          Future.sequence(config.damlPackages.map(uploadDar(_, ledger))))
        port <- startParticipant(
          config,
          indexJdbcUrl,
          ledger,
          authService,
          timeServiceBackend,
          config.seeding.map(SeedService(_)),
        )
      } yield {
        Banner.show(Console.out)
        logger.withoutContext.info(
          "Initialized sandbox version {} with ledger-id = {}, port = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}",
          BuildInfo.Version,
          ledgerId,
          port.toString,
          config.damlPackages,
          timeProviderType.description,
          ledgerType,
          authService.getClass.getSimpleName,
        )
      }
    }
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

  private def startParticipant(
      config: SandboxConfig,
      indexJdbcUrl: String,
      ledger: KeyValueParticipantState,
      authService: AuthService,
      timeServiceBackend: Option[TimeServiceBackend],
      seedService: Option[SeedService],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Int] =
    for {
      _ <- startIndexerServer(
        config = config,
        indexJdbcUrl = indexJdbcUrl,
        readService = ledger,
      )
      port <- startApiServer(
        config = config,
        indexJdbcUrl = indexJdbcUrl,
        readService = ledger,
        writeService = ledger,
        authService = authService,
        timeServiceBackend = timeServiceBackend,
        seedService = seedService
      )
    } yield port

  private def startIndexerServer(
      config: SandboxConfig,
      indexJdbcUrl: String,
      readService: ReadService,
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Unit] =
    new StandaloneIndexerServer(
      readService = readService,
      config = IndexerConfig(
        ParticipantId,
        jdbcUrl = indexJdbcUrl,
        startupMode = IndexerStartupMode.MigrateAndStart,
        allowExistingSchema = true,
      ),
      metrics = SharedMetricRegistries.getOrCreate(s"indexer-$ParticipantId"),
    )

  private def startApiServer(
      config: SandboxConfig,
      indexJdbcUrl: String,
      readService: ReadService,
      writeService: WriteService,
      authService: AuthService,
      timeServiceBackend: Option[TimeServiceBackend],
      seedService: Option[SeedService],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Int] =
    new StandaloneApiServer(
      ApiServerConfig(
        participantId = ParticipantId,
        archiveFiles = config.damlPackages,
        port = config.port,
        address = config.address,
        jdbcUrl = indexJdbcUrl,
        tlsConfig = config.tlsConfig,
        maxInboundMessageSize = config.maxInboundMessageSize,
        portFile = config.portFile,
      ),
      commandConfig = config.commandConfig,
      submissionConfig = config.submissionConfig,
      readService = readService,
      writeService = writeService,
      authService = authService,
      metrics = SharedMetricRegistries.getOrCreate(s"ledger-api-server-$ParticipantId"),
      timeServiceBackend = timeServiceBackend,
      seedService = seedService
    )
}

object Runner {
  private val logger = ContextualizedLogger.get(classOf[Runner])

  private val ParticipantId: v1.ParticipantId =
    Ref.ParticipantId.assertFromString("sandbox-participant")

  private val InMemoryLedgerJdbcUrl =
    "jdbc:sqlite:file:ledger?mode=memory&cache=shared"

  private val InMemoryIndexJdbcUrl =
    "jdbc:h2:mem:index;db_close_delay=-1;db_close_on_exit=false"
}
