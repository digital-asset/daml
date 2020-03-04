// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.{ReadService, SeedService, SubmissionId, WriteService}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.apiserver.StandaloneApiServer
import com.digitalasset.platform.indexer.StandaloneIndexerServer
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.resources.akka.AkkaResourceOwner

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class Runner[T <: ReadWriteService, Extra](
    name: String,
    factory: LedgerFactory[ReadWriteService, Extra]) {
  def owner(args: Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(name, factory.extraConfigParser, factory.defaultExtraConfig, args)
      .flatMap(owner)

  def owner(originalConfig: Config[Extra]): ResourceOwner[Unit] = {
    val config = factory.manipulateConfig(originalConfig)

    implicit val system: ActorSystem = ActorSystem(
      "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-"))
    implicit val materializer: Materializer = Materializer(system)
    implicit val executionContext: ExecutionContext = system.dispatcher

    newLoggingContext { implicit logCtx =>
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits within a `for` comprehension.
        _ <- AkkaResourceOwner.forActorSystem(() => system)
        _ <- AkkaResourceOwner.forMaterializer(() => materializer)

        // initialize all configured participants
        _ <- ResourceOwner.sequence(config.participants.map { participantConfig =>
          for {
            ledger <- factory
              .readWriteServiceOwner(config, participantConfig)
            _ <- ResourceOwner.forFuture(() =>
              Future.sequence(config.archiveFiles.map(uploadDar(_, ledger))))
            _ <- startParticipant(config, participantConfig, ledger)
          } yield ()
        })
      } yield ()
    }
  }

  private def uploadDar(from: Path, to: ReadWriteService)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
    for {
      dar <- Future(
        DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchiveFromFile(from.toFile).get)
      _ <- to.uploadPackages(submissionId, dar.all, None).toScala
    } yield ()
  }

  private def startParticipant(
      config: Config[Extra],
      participantConfig: ParticipantConfig,
      ledger: ReadWriteService)(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): ResourceOwner[Unit] =
    for {
      _ <- startIndexerServer(
        participantConfig,
        readService = ledger,
      )
      _ <- startApiServer(
        config,
        participantConfig,
        readService = ledger,
        writeService = ledger,
        authService = factory.authService(config),
        seedService = Some(SeedService(config.seeding)),
      )
    } yield ()

  private def startIndexerServer(
      config: ParticipantConfig,
      readService: ReadService,
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Unit] =
    new StandaloneIndexerServer(
      readService,
      factory.indexerConfig(config),
      factory.indexerMetricRegistry(config),
    )

  private def startApiServer(
      config: Config[Extra],
      participantConfig: ParticipantConfig,
      readService: ReadService,
      writeService: WriteService,
      authService: AuthService,
      seedService: Option[SeedService]
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Unit] =
    new StandaloneApiServer(
      factory.apiServerConfig(participantConfig, config),
      factory.commandConfig(config),
      factory.submissionConfig(config),
      readService,
      writeService,
      authService,
      factory.apiServerMetricRegistry(participantConfig),
      factory.timeServiceBackend(config),
      seedService,
    ).map(_ => ())
}
