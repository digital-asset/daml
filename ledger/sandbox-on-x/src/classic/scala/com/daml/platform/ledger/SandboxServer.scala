// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.runner.common.Config
import com.daml.ledger.participant.state.v2.WriteService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.SandboxServer._
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.{newLoggingContext, newLoggingContextWith}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, MetricsReporting}
import com.daml.platform.apiserver.ApiServer
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import com.daml.platform.sandbox.logging
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.{DbType, FlywayMigrations}
import com.daml.ports.Port
import com.daml.resources.AbstractResourceOwner
import com.daml.telemetry.NoOpTelemetryContext
import scalaz.Tag
import scalaz.syntax.tag._

import java.nio.file.Files
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.{Failure, Success, Try}

final class SandboxServer(
    config: SandboxConfig,
    metrics: Metrics,
)(implicit materializer: Materializer)
    extends ResourceOwner[Port] {

  // Only used for testing.
  def this(config: SandboxConfig, materializer: Materializer) =
    this(config, new Metrics(new MetricRegistry))(materializer)

  def acquire()(implicit resourceContext: ResourceContext): Resource[Port] = {
    val maybeLedgerId = config.jdbcUrl.flatMap(getLedgerId)
    val genericConfig = ConfigConverter.toSandboxOnXConfig(config, maybeLedgerId, DefaultName)
    for {
      participantConfig <-
        SandboxOnXRunner.validateCombinedParticipantMode(genericConfig)
      (apiServer, writeService) <-
        SandboxOnXRunner
          .buildLedger(
            genericConfig,
            participantConfig,
            materializer,
            materializer.system,
            Some(metrics),
          )
          .acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port)(resourceContext.executionContext))
      _ <- loadPackages(writeService)(resourceContext.executionContext).acquire()
    } yield {
      initializationLoggingHeader(genericConfig, apiServer)
      apiServer.port
    }
  }

  private def initializationLoggingHeader(
      genericConfig: Config[BridgeConfig],
      apiServer: ApiServer,
  ): Unit = {
    Banner.show(Console.out)
    logger.withoutContext.info(
      s"Initialized Sandbox version {} with ledger-id = {}, port = {}, index DB backend = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}, contract ids seeding = {}{}{}",
      BuildInfo.Version,
      genericConfig.ledgerId,
      apiServer.port.toString,
      DbType.jdbcType(genericConfig.participants.head.serverJdbcUrl).name,
      config.damlPackages,
      genericConfig.timeProviderType.description,
      "SQL-backed conflict-checking ledger-bridge",
      genericConfig.authService.getClass.getSimpleName,
      config.seeding.name,
      if (config.stackTraces) "" else ", stack traces = no",
      config.profileDir match {
        case None => ""
        case Some(profileDir) => s", profile directory = $profileDir"
      },
    )
    if (config.engineMode == SandboxConfig.EngineMode.EarlyAccess) {
      logger.withoutContext.warn(
        """|Using early access mode is dangerous as the backward compatibility of future SDKs is not guaranteed.
           |Should be used for testing purpose only.""".stripMargin
      )
    }
  }

  private def writePortFile(port: Port)(implicit executionContext: ExecutionContext): Future[Unit] =
    config.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.unit)

  private def loadPackages(writeService: WriteService)(implicit
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
              .asScala
          } yield ()
        }
      )
    )
}

object SandboxServer {
  private val DefaultName = LedgerName("Sandbox")
  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(config: SandboxConfig): ResourceOwner[Port] =
    owner(DefaultName, config)

  def owner(name: LedgerName, config: SandboxConfig): ResourceOwner[Port] =
    for {
      metrics <- new MetricsReporting(
        classOf[SandboxServer].getName,
        config.metricsReporter,
        config.metricsReportingInterval,
      )
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(name.unwrap.toLowerCase()))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- new SandboxServer(config, metrics)(materializer)
    } yield server

  // Run only the flyway migrations but do not initialize any of the ledger api or indexer services
  def migrateOnly(
      config: SandboxConfig
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    newLoggingContextWith(logging.participantId(config.participantId)) { implicit loggingContext =>
      logger.info("Running only schema migration scripts")
      new FlywayMigrations(config.jdbcUrl.get)
        .migrate()
    }

  // Work-around to emulate the ledgerIdMode used in sandbox-classic.
  // This is needed for the Dynamic ledger id mode, when the index should be initialized with the existing ledger id (only used in testing).
  private def getLedgerId(jdbcUrl: String): Option[String] =
    newLoggingContext { implicit loggingContext: LoggingContext =>
      Try {
        val dbType = DbType.jdbcType(jdbcUrl)

        // Creating storage backend and the data-source directly to avoid logging errors
        // on new db when creating via `IndexMetadata.read`
        val storageBackendFactory = StorageBackendFactory.of(dbType)
        val dataSource =
          storageBackendFactory.createDataSourceStorageBackend.createDataSource(jdbcUrl)

        storageBackendFactory.createParameterStorageBackend
          .ledgerIdentity(dataSource.getConnection)
          .map(_.ledgerId)
      } match {
        case Failure(err) =>
          logger.warn(
            s"Failure encountered trying to retrieve ledger id: ${err.getMessage}. Assuming uninitialized index."
          )
          None
        case Success(maybeLedgerId) =>
          maybeLedgerId.map(Tag.unwrap).filter(_.nonEmpty)
      }
    }
}
