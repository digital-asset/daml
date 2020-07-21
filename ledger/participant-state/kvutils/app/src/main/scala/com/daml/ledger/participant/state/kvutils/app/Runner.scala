// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.participant.state.v1.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.participant.state.v1.{SubmissionId, WritePackagesService}
import com.daml.lf.archive.DarReader
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.JvmMetricSet
import com.daml.platform.apiserver.StandaloneApiServer
import com.daml.platform.indexer.StandaloneIndexerServer
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.resources.akka.AkkaResourceOwner
import com.daml.resources.{Resource, ResourceOwner}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class Runner[T <: ReadWriteService, Extra](
    name: String,
    factory: LedgerFactory[ReadWriteService, Extra],
) {
  def owner(args: Seq[String]): ResourceOwner[Unit] =
    Config
      .owner(name, factory.extraConfigParser, factory.defaultExtraConfig, args)
      .flatMap(owner)

  def owner(originalConfig: Config[Extra]): ResourceOwner[Unit] = new ResourceOwner[Unit] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] = {
      val config = factory.manipulateConfig(originalConfig)

      implicit val actorSystem: ActorSystem = ActorSystem(
        "[^A-Za-z0-9_\\-]".r.replaceAllIn(name.toLowerCase, "-"))
      implicit val materializer: Materializer = Materializer(actorSystem)

      // FIXME: https://github.com/digital-asset/daml/issues/5164
      // This should be made configurable
      val engineConfig = Engine.DevConfig

      // share engine between the kvutils committer backend and the ledger api server
      // this avoids duplicate compilation of packages as well as keeping them in memory twice
      val sharedEngine = new Engine(engineConfig)

      newLoggingContext { implicit logCtx =>
        for {
          // Take ownership of the actor system and materializer so they're cleaned up properly.
          // This is necessary because we can't declare them as implicits in a `for` comprehension.
          _ <- AkkaResourceOwner.forActorSystem(() => actorSystem).acquire()
          _ <- AkkaResourceOwner.forMaterializer(() => materializer).acquire()

          // initialize all configured participants
          _ <- Resource.sequence(config.participants.map { participantConfig =>
            val metrics = factory.createMetrics(participantConfig, config)
            metrics.registry.registerAll(new JvmMetricSet)
            val lfValueTranslationCache =
              LfValueTranslation.Cache.newInstrumentedInstance(
                eventConfiguration = config.lfValueTranslationEventCache,
                contractConfiguration = config.lfValueTranslationContractCache,
                metrics = metrics,
              )
            for {
              _ <- config.metricsReporter.fold(Resource.unit)(
                reporter =>
                  ResourceOwner
                    .forCloseable(() => reporter.register(metrics.registry))
                    .map(_.start(config.metricsReportingInterval.getSeconds, TimeUnit.SECONDS))
                    .acquire())
              ledger <- factory
                .readWriteServiceOwner(config, participantConfig, sharedEngine)
                .acquire()
              readService = new TimedReadService(ledger, metrics)
              writeService = new TimedWriteService(ledger, metrics)
              _ <- Resource.fromFuture(
                Future.sequence(config.archiveFiles.map(uploadDar(_, writeService))))
              _ <- new StandaloneIndexerServer(
                readService = readService,
                config = factory.indexerConfig(participantConfig, config),
                metrics = metrics,
                lfValueTranslationCache = lfValueTranslationCache,
              ).acquire()
              _ <- new StandaloneApiServer(
                ledgerId = config.ledgerId,
                config = factory.apiServerConfig(participantConfig, config),
                commandConfig = factory.commandConfig(participantConfig, config),
                partyConfig = factory.partyConfig(config),
                ledgerConfig = factory.ledgerConfig(config),
                optWriteService = Some(writeService),
                authService = factory.authService(config),
                metrics = metrics,
                timeServiceBackend = factory.timeServiceBackend(config),
                otherInterceptors = factory.interceptors(config),
                engine = sharedEngine,
                lfValueTranslationCache = lfValueTranslationCache,
              ).acquire()
            } yield ()
          })
        } yield ()
      }
    }
  }

  private def uploadDar(from: Path, to: WritePackagesService)(
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
