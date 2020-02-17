// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{ReadService, SubmissionId, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.platform.apiserver.{ApiServerConfig, StandaloneApiServer}
import com.digitalasset.platform.indexer.{
  IndexerConfig,
  IndexerStartupMode,
  StandaloneIndexerServer
}
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.resources.akka.AkkaResourceOwner

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class Runner[T <: KeyValueLedger, Extra](name: String, factory: LedgerFactory[T, Extra]) {
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

    for {
      // Take ownership of the actor system and materializer so they're cleaned up properly.
      // This is necessary because we can't declare them as implicits within a `for` comprehension.
      _ <- AkkaResourceOwner.forActorSystem(() => system)
      _ <- AkkaResourceOwner.forMaterializer(() => materializer)

      ledgerId = config.ledgerId.getOrElse(
        Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
      readerWriter <- factory
        .owner(ledgerId, config.participantId, config.extra)
      ledger = new KeyValueParticipantState(readerWriter, readerWriter)
      _ <- ResourceOwner.forFuture(() =>
        Future.sequence(config.archiveFiles.map(uploadDar(_, ledger))))
      _ <- startParticipant(config, ledger)
    } yield ()
  }

  private def uploadDar(from: Path, to: KeyValueParticipantState)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] = {
    val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
    for {
      dar <- Future(
        DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchiveFromFile(from.toFile).get)
      _ <- to.uploadPackages(submissionId, dar.all, None).toScala
    } yield ()
  }

  private def startParticipant(config: Config[Extra], ledger: KeyValueParticipantState)(
      implicit executionContext: ExecutionContext
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

  private def startIndexerServer(
      config: Config[Extra],
      readService: ReadService,
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Unit] =
    new StandaloneIndexerServer(
      readService,
      IndexerConfig(
        config.participantId,
        jdbcUrl = config.serverJdbcUrl,
        startupMode = IndexerStartupMode.MigrateAndStart,
        allowExistingSchema = config.allowExistingSchemaForIndex,
      ),
      SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}"),
      implicitlyAllocateParties = false,
    )

  private def startApiServer(
      config: Config[Extra],
      readService: ReadService,
      writeService: WriteService,
      authService: AuthService,
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): ResourceOwner[Unit] =
    new StandaloneApiServer(
      ApiServerConfig(
        config.participantId,
        config.archiveFiles.map(_.toFile).toList,
        config.port,
        config.address,
        jdbcUrl = config.serverJdbcUrl,
        tlsConfig = None,
        TimeProvider.UTC,
        Config.DefaultMaxInboundMessageSize,
        config.portFile,
      ),
      readService,
      writeService,
      authService,
      SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}"),
    )
}
