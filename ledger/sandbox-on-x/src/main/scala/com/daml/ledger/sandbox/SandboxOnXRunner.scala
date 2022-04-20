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
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.{ReadService, Update, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.{_}
import com.daml.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.TimeServiceBackend
import com.daml.platform.store.DbType

import scala.concurrent.ExecutionContext

object SandboxOnXRunner {
  val RunnerName = "sandbox-on-x"
  private val logger = ContextualizedLogger.get(getClass)

  def owner(
      args: collection.Seq[String],
      manipulateConfig: Config[BridgeConfig] => Config[BridgeConfig] = identity,
  ): ResourceOwner[Unit] =
    Config
      .owner(
        RunnerName,
        BridgeConfigProvider.extraConfigParser,
        BridgeConfigProvider.defaultExtraConfig,
        args,
      )
      .map(manipulateConfig)
      .flatMap(owner)

  def owner(originalConfig: Config[BridgeConfig]): ResourceOwner[Unit] =
    new ResourceOwner[Unit] {
      override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
        val config = BridgeConfigProvider.manipulateConfig(originalConfig)

        config.mode match {
          case Mode.DumpIndexMetadata(jdbcUrls) =>
            DumpIndexMetadata(jdbcUrls, RunnerName)
            sys.exit(0)
          case Mode.Run =>
            run(config)
        }
      }
    }

  private def run(
      config: Config[BridgeConfig]
  )(implicit resourceContext: ResourceContext): Resource[Unit] = newLoggingContext {
    implicit loggingContext =>
      implicit val actorSystem: ActorSystem = ActorSystem(RunnerName)
      implicit val materializer: Materializer = Materializer(actorSystem)
      for {
        // Take ownership of the actor system and materializer so they're cleaned up properly.
        // This is necessary because we can't declare them as implicits in a `for` comprehension.
        _ <- ResourceOwner.forActorSystem(() => actorSystem).acquire()
        _ <- ResourceOwner.forMaterializer(() => materializer).acquire()

        participantConfig <- validateCombinedParticipantMode(config)
        // Start the ledger
        timeServiceBackendO = BridgeConfigProvider.timeServiceBackend(config)
        (stateUpdatesFeedSink, stateUpdatesSource) <- AkkaSubmissionsBridge().acquire()

        readServiceWithSubscriber: ReadService = new BridgeReadService(
          ledgerId = config.ledgerId,
          maximumDeduplicationDuration = config.maxDeduplicationDuration.getOrElse(
            BridgeConfigProvider.DefaultMaximumDeduplicationDuration
          ),
          stateUpdatesSource,
        )

        _ <- Participant
          .owner(
            config,
            participantConfig,
            BridgeConfigProvider.apiServerConfig(participantConfig, config),
            BridgeConfigProvider.indexerConfig(participantConfig, config),
            timeServiceBackendO,
            readServiceWithSubscriber,
            buildWriteService(_, _, _, _, stateUpdatesFeedSink, timeServiceBackendO)(
              materializer,
              config,
              participantConfig,
              loggingContext,
            ),
            actorSystem,
            materializer,
            config.extra.implicitPartyAllocation,
            BridgeConfigProvider.interceptors(config),
          )
          .acquire()
      } yield logInitializationHeader(config, participantConfig)
  }

  // Builds the write service and uploads the initialization DARs
  private def buildWriteService(
      metrics: Metrics,
      servicesExecutionContext: ExecutionContext,
      servicesThreadPoolSize: Int,
      indexService: IndexService,
      feedSink: Sink[(Offset, Update), NotUsed],
      timeServiceBackend: Option[TimeServiceBackend],
  )(implicit
      materializer: Materializer,
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      loggingContext: LoggingContext,
  ): ResourceOwner[WriteService] = {
    implicit val ec: ExecutionContext = servicesExecutionContext
    val bridgeMetrics = new BridgeMetrics(metrics)
    for {
      ledgerBridge <- LedgerBridge.owner(
        config,
        participantConfig,
        indexService,
        bridgeMetrics,
        servicesThreadPoolSize,
        timeServiceBackend.getOrElse(TimeProvider.UTC),
      )
      writeService <- ResourceOwner.forCloseable(() =>
        new BridgeWriteService(
          feedSink = feedSink,
          submissionBufferSize = config.extra.submissionBufferSize,
          ledgerBridge = ledgerBridge,
          bridgeMetrics = bridgeMetrics,
        )
      )
    } yield writeService
  }

  def validateCombinedParticipantMode(
      config: Config[BridgeConfig]
  ): Resource[ParticipantConfig] =
    config.participants.toList match {
      case participantConfig :: Nil if participantConfig.mode == ParticipantRunMode.Combined =>
        Resource.successful(participantConfig)
      case _ =>
        Resource.failed {
          val loggingMessage = "Sandbox-on-X can only be run in a single COMBINED participant mode."
          newLoggingContext(logger.info(loggingMessage)(_))
          new IllegalArgumentException(loggingMessage)
        }
    }

  private def logInitializationHeader(
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
  ): Unit = {
    val authentication = BridgeConfigProvider.authService(config) match {
      case _: AuthServiceJWT => "JWT-based authentication"
      case AuthServiceNone => "none authenticated"
      case _: AuthServiceStatic => "static authentication"
      case AuthServiceWildcard => "all unauthenticated allowed"
      case other => other.getClass.getSimpleName
    }

    val ledgerDetails =
      Seq[(String, String)](
        "run-mode" -> s"${participantConfig.mode} participant",
        "index DB backend" -> DbType.jdbcType(participantConfig.serverJdbcUrl).name,
        "participant-id" -> participantConfig.participantId,
        "ledger-id" -> config.ledgerId,
        "port" -> participantConfig.port.toString,
        "time mode" -> config.timeProviderType.description,
        "allowed language versions" -> s"[min = ${config.allowedLanguageVersions.min}, max = ${config.allowedLanguageVersions.max}]",
        "authentication" -> authentication,
        "contract ids seeding" -> config.seeding.toString,
      ).map { case (key, value) =>
        s"$key = $value"
      }.mkString(", ")

    logger.withoutContext.info(
      s"Initialized {} with {}, version {}, {}",
      RunnerName,
      if (config.extra.conflictCheckingEnabled) "conflict checking ledger bridge"
      else "pass-through ledger bridge (no conflict checking)",
      BuildInfo.Version,
      ledgerDetails,
    )
  }
}
