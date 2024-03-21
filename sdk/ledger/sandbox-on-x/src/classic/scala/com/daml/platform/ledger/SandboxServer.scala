// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.codahale.metrics.MetricRegistry
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.domain.PackageEntry
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.WriteService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.{CliConfigConverter, Config, ParticipantConfig}
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
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend.{DataSourceStorageBackend, StorageBackendFactory}
import com.daml.platform.store.{DbType, FlywayMigrations}
import com.daml.ports.Port
import com.daml.resources.AbstractResourceOwner
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import scalaz.Tag
import scalaz.syntax.tag._

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.chaining._
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
    val genericCliConfig = ConfigConverter.toSandboxOnXConfig(config, maybeLedgerId, DefaultName)
    val bridgeConfigAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor {
      override def authService(participantConfig: ParticipantConfig): AuthService =
        config.authService.getOrElse(AuthServiceWildcard)
    }
    val genericConfig = CliConfigConverter.toConfig(bridgeConfigAdaptor, genericCliConfig)
    for {
      (participantId, dataSource, participantConfig) <- SandboxOnXRunner.combinedParticipant(
        genericConfig
      )
      (apiServer, writeService, indexService) <-
        SandboxOnXRunner
          .buildLedger(
            participantId,
            genericConfig,
            participantConfig,
            dataSource,
            genericCliConfig.extra,
            materializer,
            materializer.system,
            bridgeConfigAdaptor,
            Some(metrics),
          )
          .acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port)(resourceContext.executionContext))
      _ <- newLoggingContextWith(logging.participantId(config.participantId)) {
        implicit loggingContext =>
          loadPackages(writeService, indexService)(
            resourceContext.executionContext,
            materializer.system,
            loggingContext,
          )
            .acquire()
      }
    } yield {
      initializationLoggingHeader(genericConfig, participantConfig, dataSource, apiServer)
      apiServer.port
    }
  }

  private def initializationLoggingHeader(
      genericConfig: Config,
      participantConfig: ParticipantConfig,
      dataSource: ParticipantDataSourceConfig,
      apiServer: ApiServer,
  ): Unit = {
    Banner.show(Console.out)
    logger.withoutContext.info(
      s"Initialized Sandbox version {} with ledger-id = {}, port = {}, index DB backend = {}, dar file = {}, time mode = {}, ledger = {}, auth-service = {}, contract ids seeding = {}{}{}",
      BuildInfo.Version,
      genericConfig.ledgerId,
      apiServer.port.toString,
      DbType
        .jdbcType(dataSource.jdbcUrl)
        .name,
      config.damlPackages,
      participantConfig.apiServer.timeProviderType.description,
      "SQL-backed conflict-checking ledger-bridge",
      participantConfig.authentication.getClass.getSimpleName,
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

  private def loadPackages(writeService: WriteService, indexService: IndexService)(implicit
      executionContext: ExecutionContext,
      system: ActorSystem,
      loggingContext: LoggingContext,
  ): AbstractResourceOwner[ResourceContext, Unit] =
    ResourceOwner.forFuture(() => {
      val packageSubmissionsTrackerMap = config.damlPackages.map { file =>
        val uploadCompletionPromise = Promise[Unit]()
        UUID.randomUUID().toString -> (file, uploadCompletionPromise)
      }.toMap

      uploadAndWaitPackages(indexService, writeService, packageSubmissionsTrackerMap)
        .tap { _ =>
          scheduleUploadTimeout(packageSubmissionsTrackerMap.iterator.map(_._2._2), 30.seconds)
        }
    })

  private def scheduleUploadTimeout(
      packageSubmissionsPromises: Iterator[Promise[Unit]],
      packageUploadTimeout: FiniteDuration,
  )(implicit
      executionContext: ExecutionContext,
      system: ActorSystem,
  ): Unit =
    packageSubmissionsPromises.foreach { uploadCompletionPromise =>
      system.scheduler.scheduleOnce(packageUploadTimeout) {
        // Package upload timeout, meant to allow fail-fast for testing
        // TODO Remove once in-memory backend (e.g. H2) is deemed stable
        uploadCompletionPromise.tryFailure(
          new RuntimeException(s"Package upload timeout after $packageUploadTimeout")
        )
        ()
      }
    }

  private def uploadAndWaitPackages(
      indexService: IndexService,
      writeService: WriteService,
      packageSubmissionsTrackerMap: Map[String, (File, Promise[Unit])],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Unit] = {
    implicit val noOpTelemetryContext: TelemetryContext = NoOpTelemetryContext

    val uploadCompletionSink = Sink.foreach[PackageEntry] {
      case PackageEntry.PackageUploadAccepted(submissionId, _) =>
        packageSubmissionsTrackerMap.get(submissionId) match {
          case Some((_, uploadCompletionPromise)) =>
            uploadCompletionPromise.complete(Success(()))
          case None =>
            throw new RuntimeException(s"Completion promise for $submissionId not found")
        }
      case PackageEntry.PackageUploadRejected(submissionId, _, reason) =>
        packageSubmissionsTrackerMap.get(submissionId) match {
          case Some((_, uploadCompletionPromise)) =>
            uploadCompletionPromise.complete(
              Failure(new RuntimeException(s"Package upload at initialization failed: $reason"))
            )
          case None =>
            throw new RuntimeException(s"Completion promise for $submissionId not found")
        }
    }

    val (killSwitch, packageEntriesStreamDone) = indexService
      .packageEntries(None)
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(uploadCompletionSink)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

    val uploadAndWaitPackagesF = Future.traverse(packageSubmissionsTrackerMap.toVector) {
      case (submissionId, (file, promise)) =>
        for {
          dar <- Future.fromTry(DarParser.readArchiveFromFile(file).toTry)
          refSubmissionId = Ref.SubmissionId.assertFromString(submissionId)
          _ <- writeService
            .uploadPackages(refSubmissionId, dar.all, None)
            .asScala
          uploadResult <- promise.future
        } yield uploadResult
    }

    uploadAndWaitPackagesF
      .map(_ => ())
      .andThen { case _ =>
        killSwitch.shutdown()
        packageEntriesStreamDone
      }
  }
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
          storageBackendFactory.createDataSourceStorageBackend.createDataSource(
            DataSourceStorageBackend.DataSourceConfig(jdbcUrl)
          )

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
