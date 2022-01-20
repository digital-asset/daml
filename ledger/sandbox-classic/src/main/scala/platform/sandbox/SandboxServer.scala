// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.app._
import com.daml.ledger.participant.state.v2.WriteService
import com.daml.ledger.resources
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.{BridgeConfig, SandboxOnXRunner}
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.{newLoggingContext, newLoggingContextWith}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, MetricsReporting}
import com.daml.platform.apiserver._
import com.daml.platform.sandbox.SandboxServer._
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{FlywayMigrations, IndexMetadata}
import com.daml.ports.Port
import com.daml.resources.AbstractResourceOwner
import com.daml.telemetry.NoOpTelemetryContext
import scalaz.syntax.tag._

import java.nio.file.Files
import java.util.UUID
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object SandboxServer {
  private val DefaultName = LedgerName("Sandbox")

  private val AsyncTolerance = 30.seconds

  private val logger = ContextualizedLogger.get(this.getClass)

  // Only used for testing.
  def owner(config: SandboxConfig): ResourceOwner[SandboxServer] =
    owner(DefaultName, config)

  def owner(name: LedgerName, config: SandboxConfig): ResourceOwner[SandboxServer] =
    for {
      metrics <- new MetricsReporting(
        classOf[SandboxServer].getName,
        config.metricsReporter,
        config.metricsReportingInterval,
      )
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(name.unwrap.toLowerCase()))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- ResourceOwner
        .forTryCloseable(() => Try(new SandboxServer(config, materializer, metrics)))
      // Wait for the API server to start.
      _ <- new ResourceOwner[Unit] {
        override def acquire()(implicit context: ResourceContext): Resource[Unit] =
          // We use the Future rather than the Resource to avoid holding onto the API server.
          // Otherwise, we cause a memory leak upon reset.
          Resource.fromFuture(server.apiServer.map(_ => ()))
      }
    } yield server

  // Run only the flyway migrations but do not initialize any of the ledger api or indexer services
  def migrateOnly(
      config: SandboxConfig
  )(implicit resourceContext: ResourceContext): Future[Unit] = {

    newLoggingContextWith(logging.participantId(config.participantId)) { implicit loggingContext =>
      logger.info("Running only schema migration scripts")
      new FlywayMigrations(config.jdbcUrl.get)
        .migrate()
    }
  }

  final class SandboxState(
      apiServerResource: Resource[ApiServer]
  ) {

    def port(implicit executionContext: ExecutionContext): Future[Port] =
      apiServer.map(_.port)

    private[SandboxServer] def apiServer: Future[ApiServer] =
      apiServerResource.asFuture

    def release(): Future[Unit] =
      apiServerResource.release()
  }

}

final class SandboxServer(
    config: SandboxConfig,
    materializer: Materializer,
    metrics: Metrics,
) extends AutoCloseable {
  // Only used for testing.
  def this(config: SandboxConfig, materializer: Materializer) =
    this(config, materializer, new Metrics(new MetricRegistry))

  // We store a Future rather than a Resource to avoid keeping old resources around after a reset.
  // It's package-private so we can test that we drop the reference properly in ResetServiceIT.
  @volatile
  private[sandbox] var sandboxState: Future[SandboxState] = start(
    ResourceContext(materializer.executionContext),
    materializer.executionContext,
  )

  private def apiServer(implicit executionContext: ExecutionContext): Future[ApiServer] =
    sandboxState.flatMap(_.apiServer)

  // Only used in testing; hopefully we can get rid of it soon.
  def port: Port =
    Await.result(portF(ExecutionContext.parasitic), AsyncTolerance)

  def portF(implicit executionContext: ExecutionContext): Future[Port] =
    apiServer.map(_.port)

  private def start(implicit
      resourceContext: ResourceContext,
      executionContext: ExecutionContext,
  ): Future[SandboxState] = {
    val apiServerResource = for {
      maybeLedgerId <- config.jdbcUrl
        .map(url => Utils.getLedgerId(url))
        .getOrElse(Resource.successful(None))
      genericConfig = ConfigConverter.toSandboxOnXConfig(config, maybeLedgerId)
      participantConfig <- SandboxOnXRunner.validateCombinedParticipantMode(genericConfig)
      api <- apiServer(genericConfig, participantConfig)
    } yield api

    Future.successful(new SandboxState(apiServerResource))
  }

  private def apiServer(genericConfig: Config[BridgeConfig], participantConfig: ParticipantConfig)(
      implicit
      resourceContext: ResourceContext,
      executionContext: ExecutionContext,
  ) =
    for {
      (apiServer, writeService) <- SandboxOnXRunner
        .buildLedger(
          genericConfig,
          participantConfig,
          materializer,
          materializer.system,
          Some(metrics),
        )
        .acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port))
      _ <- loadPackages(writeService).acquire()
    } yield apiServer

  override def close(): Unit = {
    Await.result(sandboxState.flatMap(_.release())(ExecutionContext.parasitic), AsyncTolerance)
  }

  private def writePortFile(port: Port)(implicit executionContext: ExecutionContext): Future[Unit] =
    config.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.unit)

  def loadPackages(writeService: WriteService)(implicit
      executionContext: ExecutionContext
  ): AbstractResourceOwner[ResourceContext, List[Unit]] =
    ResourceOwner.forFuture(() =>
      Future.sequence(
        config.damlPackages.map { file =>
          val submissionId = Ref.SubmissionId.assertFromString(UUID.randomUUID().toString)
          for {
            dar <- Future.fromTry(DarParser.readArchiveFromFile(file).toTry)
            _ <- writeService
              .uploadPackages(submissionId, dar.all, None)(
                LoggingContext.ForTesting,
                NoOpTelemetryContext,
              )
              .toScala
          } yield ()
        }
      )
    )
}

object Utils {
  // TODO SoX-to-sandbox-classic: Ugly work-around to emulate the ledgerIdMode used in sandbox-classic
  def getLedgerId(
      jdbcUrl: String
  )(implicit resourceContext: ResourceContext): resources.Resource[Option[String]] = {
    implicit val actorSystem: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(actorSystem)
    implicit val ec: ExecutionContextExecutor = materializer.executionContext

    newLoggingContext { implicit loggingContext: LoggingContext =>
      Resource
        .fromFuture(IndexMetadata.read(jdbcUrl, ErrorFactories(true)).transform {
          case Failure(_) => Success(None)
          case Success(indexMetadata) =>
            if (indexMetadata.ledgerId.isEmpty) Success(None)
            else Success(Some(indexMetadata.ledgerId))
        })
    }
  }
}
