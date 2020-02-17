// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.io.File
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.server.damlonx.reference.v2.cli.Cli
import com.daml.ledger.participant.state.v1.{ReadService, SubmissionId, WriteService}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.apiserver.{ApiServerConfig, StandaloneApiServer}
import com.digitalasset.platform.indexer.{IndexerConfig, StandaloneIndexerServer}
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{ProgramResource, ResourceOwner}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object ReferenceServer {
  private implicit val system: ActorSystem = ActorSystem("indexed-kvutils")
  private implicit val materializer: Materializer = Materializer(system)
  private implicit val executionContext: ExecutionContext = system.dispatcher

  def main(args: Array[String]): Unit = {
    val config =
      Cli
        .parse(
          args,
          binaryName = "damlonx-reference-server",
          description = "A fully compliant DAML Ledger API server backed by an in-memory store.",
        )
        .getOrElse(sys.exit(1))

    val owner =
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits within a `for` comprehension.
        _ <- AkkaResourceOwner.forActorSystem(() => system)
        _ <- AkkaResourceOwner.forMaterializer(() => materializer)
        ledger <- ResourceOwner
          .forCloseable(() => new InMemoryKVParticipantState(config.participantId))
        _ <- ResourceOwner.forFuture(() =>
          Future.sequence(config.archiveFiles.map(uploadDar(_, ledger))))
        _ <- startParticipant(config, ledger)
        _ <- ResourceOwner.sequenceIgnoringValues(
          extraParticipantConfig(config).map(startParticipant(_, ledger)))
      } yield ()

    new ProgramResource(owner).run()
  }

  private def uploadDar(from: File, to: InMemoryKVParticipantState): Future[Unit] = {
    val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
    for {
      dar <- Future(
        DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchiveFromFile(from).get)
      _ <- to.uploadPackages(submissionId, dar.all, None).toScala
    } yield ()
  }

  private def extraParticipantConfig(base: Config): Vector[Config] =
    for ((extraParticipantId, port, jdbcUrl) <- base.extraParticipants)
      yield
        base.copy(
          port = port,
          participantId = extraParticipantId,
          jdbcUrl = jdbcUrl,
        )

  private def startParticipant(
      config: Config,
      ledger: InMemoryKVParticipantState
  ): ResourceOwner[Unit] =
    newLoggingContext { implicit logCtx =>
      for {
        _ <- startIndexerServer(config, readService = ledger)
        _ <- startApiServer(
          config,
          readService = ledger,
          writeService = ledger,
          authService = AuthServiceWildcard,
        )
      } yield ()
    }

  private def startIndexerServer(config: Config, readService: ReadService)(
      implicit logCtx: LoggingContext
  ): ResourceOwner[Unit] =
    new StandaloneIndexerServer(
      readService,
      IndexerConfig(config.participantId, config.jdbcUrl, config.startupMode),
      SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}"),
      implicitlyAllocateParties = false,
    )

  private def startApiServer(
      config: Config,
      readService: ReadService,
      writeService: WriteService,
      authService: AuthService,
  )(implicit logCtx: LoggingContext): ResourceOwner[Unit] =
    new StandaloneApiServer(
      ApiServerConfig(
        config.participantId,
        config.archiveFiles,
        config.port,
        config.address,
        config.jdbcUrl,
        config.tlsConfig,
        config.timeProvider,
        config.maxInboundMessageSize,
        config.portFile,
      ),
      readService,
      writeService,
      authService,
      SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}"),
    )
}
