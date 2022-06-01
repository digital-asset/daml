// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.{Config, ParticipantConfig}
import com.daml.ledger.sandbox.NewSandboxServer._
import com.daml.lf.language.LanguageVersion
import com.daml.logging.ContextualizedLogger
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.MetricsReporting
import com.daml.platform.apiserver.{ApiServer, ApiServerConfig}
import com.daml.platform.sandbox.banner.Banner
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.logging
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.DbType
import scala.concurrent.duration._
import com.daml.ports.Port
import scalaz.syntax.tag._

import java.io.File

final class NewSandboxServer(
    genericConfig: Config,
    bridgeConfig: BridgeConfig,
    authServiceFromConfig: Option[AuthService],
    damlPackages: List[File],
)(implicit materializer: Materializer)
    extends ResourceOwner[Port] {

  def acquire()(implicit resourceContext: ResourceContext): Resource[Port] = {
    val bridgeConfigAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor {
      override def authService(apiServerConfig: ApiServerConfig): AuthService =
        authServiceFromConfig.getOrElse(AuthServiceWildcard)
    }
    for {
      (participantId, dataSource, participantConfig) <- SandboxOnXRunner.combinedParticipant(
        genericConfig
      )
      metrics <- new MetricsReporting(
        classOf[NewSandboxServer].getName,
        None,
        10.seconds,
      ).acquire()

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
      _ <- newLoggingContextWith(
        logging.participantId(participantId)
      ) { implicit loggingContext =>
        new PackageUploader(writeService, indexService)
          .upload(damlPackages)
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
}

object NewSandboxServer {
  case class CustomConfig(
      genericConfig: Config,
      bridgeConfig: BridgeConfig,
      authServiceFromConfig: Option[AuthService] = None,
      damlPackages: List[File] = List.empty,
  )
  private val DefaultName = LedgerName("Sandbox")
  private val logger = ContextualizedLogger.get(this.getClass)

  def owner(config: NewSandboxServer.CustomConfig): ResourceOwner[Port] =
    owner(DefaultName, config)

  private def owner(name: LedgerName, config: NewSandboxServer.CustomConfig): ResourceOwner[Port] =
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(name.unwrap.toLowerCase()))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- new NewSandboxServer(
        config.genericConfig,
        config.bridgeConfig,
        config.authServiceFromConfig,
        config.damlPackages,
      )(materializer)
    } yield server

}
