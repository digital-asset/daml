// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.SubmissionId
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.apiserver.StandaloneApiServer
import com.digitalasset.platform.indexer.StandaloneIndexerServer
import com.digitalasset.resources.akka.AkkaResourceOwner
import com.digitalasset.resources.{Resource, ResourceOwner}

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

  def owner(originalConfig: Config[Extra]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] = {
      val config = factory.manipulateConfig(originalConfig)

      implicit val system: ActorSystem = ActorSystem(
        "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-"))
      implicit val materializer: Materializer = Materializer(system)

      newLoggingContext { implicit logCtx =>
        for {
          // Take ownership of the actor system and materializer so they're cleaned up properly.
          // This is necessary because we can't declare them as implicits in a `for` comprehension.
          _ <- AkkaResourceOwner.forActorSystem(() => system).acquire()
          _ <- AkkaResourceOwner.forMaterializer(() => materializer).acquire()

          // initialize all configured participants
          _ <- Resource.sequence(config.participants.map { participantConfig =>
            for {
              ledger <- factory.readWriteServiceOwner(config, participantConfig).acquire()
              _ <- Resource.fromFuture(
                Future.sequence(config.archiveFiles.map(uploadDar(_, ledger))))
              _ <- new StandaloneIndexerServer(
                system,
                readService = ledger,
                factory.indexerConfig(participantConfig, config),
                factory.indexerMetricRegistry(participantConfig, config),
              ).acquire()
              _ <- new StandaloneApiServer(
                factory.apiServerConfig(participantConfig, config),
                factory.commandConfig(config),
                factory.partyConfig(config),
                factory.submissionConfig(config),
                readService = ledger,
                writeService = ledger,
                authService = factory.authService(config),
                factory.apiServerMetricRegistry(participantConfig, config),
                factory.timeServiceBackend(config),
                Some(config.seeding),
              ).acquire()
            } yield ()
          })
        } yield ()
      }
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
}
