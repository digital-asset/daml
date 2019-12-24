// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.io.File
import java.nio.file.Path
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService, SubmissionId, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.digitalasset.platform.apiserver.{ApiServerConfig, StandaloneApiServer}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.indexer.{
  IndexerConfig,
  IndexerStartupMode,
  StandaloneIndexerServer
}
import com.digitalasset.platform.resources.{Resource, ResourceOwner}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

object Main extends App {
  case class Config(
      participantId: ParticipantId,
      port: Int,
      portFile: Option[Path],
      archiveFiles: Seq[Path],
  )

  object Config {
    val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

    def default: Config =
      Config(
        participantId = ParticipantId.assertFromString("example"),
        port = 6865,
        portFile = None,
        archiveFiles = Vector.empty,
      )

    def parse(): Option[Config] = parser.parse(args, default)

    private val parser: OptionParser[Config] = new scopt.OptionParser[Config]("in-memory ledger") {
      head("In-memory ledger")

      opt[String](name = "participant-id")
        .optional()
        .text("The participant ID given to all components of the ledger API server.")
        .action((participantId, config) =>
          config.copy(participantId = ParticipantId.assertFromString(participantId)))

      opt[Int]("port")
        .optional()
        .text("The port on which to run the ledger API server.")
        .action((port, config) => config.copy(port = port))
        .withFallback(() => 6865)

      opt[File]("port-file")
        .optional()
        .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")
        .action((file, config) => config.copy(portFile = Some(file.toPath)))

      arg[File]("<archive>...")
        .optional()
        .unbounded()
        .text("DAR files to load. Scenarios are ignored. The servers starts with an empty ledger by default.")
        .action((file, config) => config.copy(archiveFiles = config.archiveFiles :+ file.toPath))

      help("help").text("Runs the in-memory ledger as a service.")
    }
  }

  val config = Config.parse().getOrElse(sys.exit(1))

  val logger = LoggerFactory.getLogger(getClass)

  implicit val system: ActorSystem = ActorSystem("app")
  implicit val materializer: Materializer = Materializer(system)
  implicit val executionContext: ExecutionContext = system.dispatcher

  val resource = for {
    // Take ownership of the actor system and materializer so they're cleaned up properly.
    // This is necessary because we can't declare them as implicits within a `for` comprehension.
    _ <- ResourceOwner.forActorSystem(() => system).acquire()
    _ <- ResourceOwner.forMaterializer(() => materializer).acquire()
    readerWriter <- ResourceOwner
      .forCloseable(() => new InMemoryLedgerReaderWriter(participantId = config.participantId))
      .acquire()
    ledger = new KeyValueParticipantState(readerWriter, readerWriter)
    _ = config.archiveFiles.foreach { file =>
      val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
      for {
        dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
          .readArchiveFromFile(file.toFile)
      } yield ledger.uploadPackages(submissionId, dar.all, None)
    }
    _ <- startIndexerServer(config, readService = ledger)
    _ <- startApiServer(
      config,
      readService = ledger,
      writeService = ledger,
      authService = AuthServiceWildcard,
    )
  } yield ()

  resource.asFuture.failed.foreach { exception =>
    logger.error("Shutting down because of an initialization error.", exception)
    System.exit(1)
  }

  Runtime.getRuntime.addShutdownHook(new Thread(() => Await.result(resource.release(), 10.seconds)))

  private def startIndexerServer(config: Config, readService: ReadService): Resource[Unit] =
    new StandaloneIndexerServer(
      readService,
      IndexerConfig(
        config.participantId,
        jdbcUrl = "jdbc:h2:mem:server;db_close_delay=-1;db_close_on_exit=false",
        startupMode = IndexerStartupMode.MigrateAndStart,
      ),
      NamedLoggerFactory.forParticipant(config.participantId),
      SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}"),
    ).acquire()

  private def startApiServer(
      config: Config,
      readService: ReadService,
      writeService: WriteService,
      authService: AuthService,
  ): Resource[Unit] =
    new StandaloneApiServer(
      ApiServerConfig(
        config.participantId,
        config.archiveFiles.map(_.toFile).toList,
        config.port,
        jdbcUrl = "jdbc:h2:mem:server;db_close_delay=-1;db_close_on_exit=false",
        tlsConfig = None,
        TimeProvider.UTC,
        Config.DefaultMaxInboundMessageSize,
        config.portFile,
      ),
      readService,
      writeService,
      authService,
      NamedLoggerFactory.forParticipant(config.participantId),
      SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}"),
    ).acquire()
}
