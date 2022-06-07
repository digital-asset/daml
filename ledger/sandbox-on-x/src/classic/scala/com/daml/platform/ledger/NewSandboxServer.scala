// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.api.domain.PackageEntry
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.WriteService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.{Config, ParticipantConfig}
import com.daml.ledger.sandbox.NewSandboxServer._
import com.daml.ledger.sandbox.SandboxOnXRunner.validateDataSource
import com.daml.lf.archive.DarParser
import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, MetricsReporter, MetricsReporting}
import com.daml.platform.apiserver.{ApiServer, ApiServerConfig}
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.logging
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.DbType
import com.daml.ports.Port
import com.daml.resources.AbstractResourceOwner
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import scalaz.syntax.tag._

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.chaining._
import scala.util.{Failure, Success}

final class NewSandboxServer(
    genericConfig: Config,
    bridgeConfig: BridgeConfig,
    authServiceFromConfig: Option[AuthService],
    damlPackages: List[File],
    metrics: Metrics,
)(implicit materializer: Materializer)
    extends ResourceOwner[Port] {

  def acquire()(implicit resourceContext: ResourceContext): Resource[Port] = {
    val bridgeConfigAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor {
      override def authService(apiServerConfig: ApiServerConfig): AuthService =
        authServiceFromConfig.getOrElse(AuthServiceWildcard)
    }
    for {
      (participantId, participantConfig) <-
        SandboxOnXRunner.validateCombinedParticipantMode(genericConfig)
      dataSource <- validateDataSource(genericConfig, participantId)
      (apiServer, writeService, indexService) <-
        SandboxOnXRunner
          .buildLedger(
            participantId,
            genericConfig,
            participantConfig,
            dataSource,
            bridgeConfig,
            materializer,
            materializer.system,
            bridgeConfigAdaptor,
            Some(metrics),
          )
          .acquire()
      _ <- Resource.fromFuture(writePortFile(apiServer.port)(resourceContext.executionContext))
      _ <- newLoggingContextWith(
        logging.participantId(participantId)
      ) { implicit loggingContext =>
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
      damlPackages,
      participantConfig.apiServer.timeProviderType.description,
      "SQL-backed conflict-checking ledger-bridge",
      participantConfig.apiServer.authentication.getClass.getSimpleName,
      participantConfig.apiServer.seeding.name,
      if (genericConfig.engine.stackTraceMode) "" else ", stack traces = no",
      genericConfig.engine.profileDir match {
        case None => ""
        case Some(profileDir) => s", profile directory = $profileDir"
      },
    )
    if (genericConfig.engine.allowedLanguageVersions == LanguageVersion.EarlyAccessVersions) {
      logger.withoutContext.warn(
        """|Using early access mode is dangerous as the backward compatibility of future SDKs is not guaranteed.
           |Should be used for testing purpose only.""".stripMargin
      )
    }
  }

  private def writePortFile(port: Port)(implicit executionContext: ExecutionContext): Future[Unit] =
    genericConfig.participants.values.head.apiServer.portFile
      .map(path => Future(Files.write(path, Seq(port.toString).asJava)).map(_ => ()))
      .getOrElse(Future.unit)

  private def loadPackages(writeService: WriteService, indexService: IndexService)(implicit
      executionContext: ExecutionContext,
      system: ActorSystem,
      loggingContext: LoggingContext,
  ): AbstractResourceOwner[ResourceContext, Unit] =
    ResourceOwner.forFuture(() => {
      val packageSubmissionsTrackerMap = damlPackages.map { file =>
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

object NewSandboxServer {
  case class CustomConfig(
      genericConfig: Config,
      bridgeConfig: BridgeConfig,
      authServiceFromConfig: Option[AuthService] = None,
      damlPackages: List[File] = List.empty,
      metricsReporter: Option[MetricsReporter] = None,
      metricsReportingInterval: FiniteDuration = 10.seconds,
  )
  private val DefaultName = LedgerName("Sandbox")
  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(config: NewSandboxServer.CustomConfig): ResourceOwner[Port] =
    owner(DefaultName, config)

  def owner(name: LedgerName, config: NewSandboxServer.CustomConfig): ResourceOwner[Port] =
    for {
      metrics <- new MetricsReporting(
        classOf[NewSandboxServer].getName,
        config.metricsReporter,
        config.metricsReportingInterval,
      )
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(name.unwrap.toLowerCase()))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- new NewSandboxServer(
        config.genericConfig,
        config.bridgeConfig,
        config.authServiceFromConfig,
        config.damlPackages,
        metrics,
      )(materializer)
    } yield server

}
