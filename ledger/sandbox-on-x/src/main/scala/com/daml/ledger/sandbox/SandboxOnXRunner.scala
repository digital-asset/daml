// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.{
  AuthServiceJWT,
  AuthServiceNone,
  AuthServiceStatic,
  AuthServiceWildcard,
}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common._
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.ParticipantServer
import com.daml.platform.ParticipantServer.BuildWriteService
import com.daml.platform.config.ParticipantConfig
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.DbType
import com.daml.ports.Port

import scala.util.Try

object SandboxOnXRunner {
  val RunnerName = "sandbox-on-x"
  private val logger = ContextualizedLogger.get(getClass)

  def owner(
      configAdaptor: BridgeConfigAdaptor,
      config: Config,
      bridgeConfig: BridgeConfig,
  ): ResourceOwner[Port] =
    new ResourceOwner[Port] {
      override def acquire()(implicit context: ResourceContext): Resource[Port] =
        SandboxOnXRunner.run(configAdaptor, config, bridgeConfig)
    }

  def run(
      configAdaptor: BridgeConfigAdaptor,
      config: Config,
      bridgeConfig: BridgeConfig,
  )(implicit resourceContext: ResourceContext): Resource[Port] = newLoggingContext {
    implicit loggingContext =>
      implicit val actorSystem: ActorSystem = ActorSystem(RunnerName)
      implicit val materializer: Materializer = Materializer(actorSystem)

      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits in a `for` comprehension.
        _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
        _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

        (participantId, dataSource, participantConfig) <- combinedParticipant(config)

        (stateUpdatesFeedSink, stateUpdatesSource) <- AkkaSubmissionsBridge().acquire()
        buildWriteServiceLambda = buildWriteService(
          participantId = participantId,
          feedSink = stateUpdatesFeedSink,
          participantConfig = participantConfig,
          bridgeConfig = bridgeConfig,
          materializer = materializer,
          loggingContext = loggingContext,
        )
        readServiceWithSubscriber = new BridgeReadService(
          ledgerId = config.ledgerId,
          maximumDeduplicationDuration = bridgeConfig.maxDeduplicationDuration,
          stateUpdatesSource,
        )
        (apiServer, _, _) <- new ParticipantServer(
          participantId,
          config.ledgerId,
          participantConfig,
          config.engine,
          config.metrics,
          dataSource,
          buildWriteServiceLambda,
          readServiceWithSubscriber,
          configAdaptor.timeServiceBackend(participantConfig.apiServer),
          configAdaptor.authService(participantConfig),
        )(materializer, actorSystem).owner.acquire()
      } yield {
        logInitializationHeader(
          config,
          participantId,
          participantConfig,
          dataSource,
          bridgeConfig,
        )
        apiServer.port
      }
  }

  def combinedParticipant(
      config: Config
  )(implicit
      resourceContext: ResourceContext
  ): Resource[(Ref.ParticipantId, ParticipantDataSourceConfig, ParticipantConfig)] = for {
    (participantId, participantConfig) <- validateCombinedParticipantMode(config)
    dataSource <- validateDataSource(config, participantId)
  } yield (participantId, dataSource, participantConfig)

  private def validateDataSource(
      config: Config,
      participantId: Ref.ParticipantId,
  ): Resource[ParticipantDataSourceConfig] =
    Resource.fromTry(
      Try(
        config.dataSource.getOrElse(
          participantId,
          throw new IllegalArgumentException(
            s"Data Source has not been provided for participantId=$participantId"
          ),
        )
      )
    )

  private def validateCombinedParticipantMode(
      config: Config
  ): Resource[(Ref.ParticipantId, ParticipantConfig)] =
    config.participants.toList match {
      case (participantId, participantConfig) :: Nil =>
        Resource.successful((participantId, participantConfig))
      case _ =>
        Resource.failed {
          val loggingMessage = "Sandbox-on-X can only be run with a single participant."
          newLoggingContext(logger.info(loggingMessage)(_))
          new IllegalArgumentException(loggingMessage)
        }
    }

  // Builds the write service and uploads the initialization DARs
  def buildWriteService(
      participantId: Ref.ParticipantId,
      feedSink: Sink[(Offset, Update), NotUsed],
      participantConfig: ParticipantConfig,
      bridgeConfig: BridgeConfig,
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): BuildWriteService = {
    case (
          indexService,
          metrics,
          servicesExecutionContext,
          servicesThreadPoolSize,
          timeServiceBackend,
        ) =>
      val bridgeMetrics = new BridgeMetrics(metrics)
      for {
        ledgerBridge <- LedgerBridge.owner(
          participantId,
          participantConfig,
          bridgeConfig,
          indexService,
          bridgeMetrics,
          servicesThreadPoolSize,
          timeServiceBackend.getOrElse(TimeProvider.UTC),
        )(loggingContext, servicesExecutionContext)
        writeService <- ResourceOwner.forCloseable(() =>
          new BridgeWriteService(
            feedSink = feedSink,
            submissionBufferSize = bridgeConfig.submissionBufferSize,
            ledgerBridge = ledgerBridge,
            bridgeMetrics = bridgeMetrics,
          )(materializer, loggingContext)
        )
      } yield writeService
  }

  private def logInitializationHeader(
      config: Config,
      participantId: Ref.ParticipantId,
      participantConfig: ParticipantConfig,
      participantDataSourceConfig: ParticipantDataSourceConfig,
      extra: BridgeConfig,
  ): Unit = {
    val apiServerConfig = participantConfig.apiServer
    val authentication = participantConfig.authentication.create() match {
      case _: AuthServiceJWT => "JWT-based authentication"
      case AuthServiceNone => "none authenticated"
      case _: AuthServiceStatic => "static authentication"
      case AuthServiceWildcard => "all unauthenticated allowed"
      case other => other.getClass.getSimpleName
    }

    val ledgerDetails =
      Seq[(String, String)](
        "index DB backend" -> DbType
          .jdbcType(participantDataSourceConfig.jdbcUrl)
          .name,
        "participant-id" -> participantId,
        "ledger-id" -> config.ledgerId,
        "port" -> apiServerConfig.port.toString,
        "time mode" -> apiServerConfig.timeProviderType.description,
        "allowed language versions" -> s"[min = ${config.engine.allowedLanguageVersions.min}, max = ${config.engine.allowedLanguageVersions.max}]",
        "authentication" -> authentication,
        "contract ids seeding" -> apiServerConfig.seeding.toString,
      ).map { case (key, value) =>
        s"$key = $value"
      }.mkString(", ")

    logger.withoutContext.info(
      s"Initialized {} with {}, version {}, {}",
      RunnerName,
      if (extra.conflictCheckingEnabled) "conflict checking ledger bridge"
      else "pass-through ledger bridge (no conflict checking)",
      BuildInfo.Version,
      ledgerDetails,
    )
  }
}
