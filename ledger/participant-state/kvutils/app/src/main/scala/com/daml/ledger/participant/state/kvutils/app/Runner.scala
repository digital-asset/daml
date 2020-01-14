// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

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

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

class Runner(name: String, construct: ParticipantId => KeyValueLedger) {
  def run(args: Seq[String]): Unit = {
    val config = Config.parse(name, args).getOrElse(sys.exit(1))

    val logger = LoggerFactory.getLogger(getClass)

    implicit val system: ActorSystem = ActorSystem(
      "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-"))
    implicit val materializer: Materializer = Materializer(system)
    implicit val executionContext: ExecutionContext = system.dispatcher

    val resource = for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits within a `for` comprehension.
      _ <- ResourceOwner.forActorSystem(() => system).acquire()
      _ <- ResourceOwner.forMaterializer(() => materializer).acquire()
      readerWriter <- ResourceOwner
        .forCloseable(() => construct(config.participantId))
        .acquire()
      ledger = new KeyValueParticipantState(readerWriter, readerWriter)
      _ <- Resource.sequenceIgnoringValues(config.archiveFiles.map { file =>
        val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
        for {
          dar <- ResourceOwner
            .forTry(() =>
              DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
                .readArchiveFromFile(file.toFile))
            .acquire()
          _ <- ResourceOwner
            .forCompletionStage(() => ledger.uploadPackages(submissionId, dar.all, None))
            .acquire()
        } yield ()
      })
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

    Runtime.getRuntime
      .addShutdownHook(new Thread(() => Await.result(resource.release(), 10.seconds)))
  }

  private def startIndexerServer(
      config: Config,
      readService: ReadService,
  )(implicit executionContext: ExecutionContext): Resource[Unit] =
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
  )(implicit executionContext: ExecutionContext): Resource[Unit] =
    new StandaloneApiServer(
      ApiServerConfig(
        config.participantId,
        config.archiveFiles.map(_.toFile).toList,
        config.port,
        config.address,
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
