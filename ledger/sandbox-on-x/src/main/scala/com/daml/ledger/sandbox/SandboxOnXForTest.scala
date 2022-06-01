// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.Config
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.MetricsReporting
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.logging
import com.daml.ports.Port
import scalaz.syntax.tag._

import java.io.File
import java.util.UUID
import scala.concurrent.duration._

class SandboxOnXForTest(
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
        classOf[SandboxOnXForTest].getName,
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
      apiServer.port
    }
  }

}

object SandboxOnXForTest {
  def defaultH2SandboxJdbcUrl() =
    s"jdbc:h2:mem:sandbox-${UUID.randomUUID().toString};db_close_delay=-1"

  case class CustomConfig(
      genericConfig: Config,
      bridgeConfig: BridgeConfig,
      authServiceFromConfig: Option[AuthService] = None,
      damlPackages: List[File] = List.empty,
  )
  private val DefaultName = LedgerName("Sandbox")

  def owner(config: SandboxOnXForTest.CustomConfig): ResourceOwner[Port] =
    owner(DefaultName, config)

  private def owner(name: LedgerName, config: SandboxOnXForTest.CustomConfig): ResourceOwner[Port] =
    for {
      actorSystem <- ResourceOwner.forActorSystem(() => ActorSystem(name.unwrap.toLowerCase()))
      materializer <- ResourceOwner.forMaterializer(() => Materializer(actorSystem))
      server <- new SandboxOnXForTest(
        config.genericConfig,
        config.bridgeConfig,
        config.authServiceFromConfig,
        config.damlPackages,
      )(materializer)
    } yield server

}
